"""Verify Internet Archive releases against independent publisher evidence."""

import json
import re
import time
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from html import unescape
from pathlib import Path, PurePosixPath
from types import TracebackType
from typing import Protocol, cast
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from sql.archive.discovery import DiscoveryCache, DiscoveryEntry
from sql.domain.models import SnapshotIdentity
from utils.backoff import archive_http_delay
from utils.config import validate_item_id
from utils.logging_utils import ProgressReporter, format_bytes, get_logger
from utils.manifest import SnapshotLock
from utils.types import JSONObject, JSONValue, canonical_hash, json_array, json_integer, json_object, json_string

LOGGER = get_logger(__name__)


class HTTPResponse(Protocol):
    @property
    def status(self) -> int:
        """The HTTP response status code."""
        ...

    @property
    def headers(self) -> Mapping[str, str]:
        """The HTTP response headers."""
        ...

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes, or all remaining bytes when size is negative."""
        ...

    def __enter__(self) -> HTTPResponse:
        """Provide the open response for a context-managed read."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the response when its context exits."""
        ...


class TransferClient(Protocol):
    def open(self, url: str, headers: dict[str, str] | None = None) -> HTTPResponse:
        """Open a streamed HTTP response with optional request headers."""
        ...


class MetadataClient(Protocol):
    def json(self, url: str) -> JSONObject:
        """Fetch a URL and decode its JSON object."""
        ...


def _items(document: JSONObject) -> list[JSONObject]:
    """Decode the document's items array as JSON objects."""
    return [json_object(item) for item in json_array(document.get("items", []))]


def _published_size(value: JSONValue) -> int:
    """Parse an archive size supplied as an integer or integer string."""
    if isinstance(value, (str, int)):
        return int(value)
    raise ValueError("Archive size must be an integer or integer string")


class HTTPClient:
    def __init__(self, timeout: float = 30, attempts: int = 3, max_metadata_bytes: int = 8 * 1024**2) -> None:
        """Set request timeout, retry count, and metadata byte limit."""
        self.timeout, self.attempts, self.max_metadata_bytes = timeout, attempts, max_metadata_bytes

    def open(self, url: str, headers: dict[str, str] | None = None) -> HTTPResponse:
        """Open a response with identity encoding and the configured timeout."""
        return cast(
            HTTPResponse,
            urlopen(
                Request(
                    url,
                    headers={"User-Agent": "Stack2Graph/0.1", "Accept-Encoding": "identity", **(headers or {})},
                ),
                timeout=self.timeout,
            ),
        )

    def json(self, url: str) -> JSONObject:
        """Fetch bounded JSON metadata, retrying operating-system errors."""
        for attempt in range(self.attempts):
            try:
                with self.open(url) as response:
                    body = response.read(self.max_metadata_bytes + 1)
                if len(body) > self.max_metadata_bytes:
                    raise ValueError(f"Metadata exceeds bounded read budget: {url}")
                value = cast(JSONValue, json.loads(body))
                if not isinstance(value, dict):
                    raise ValueError(f"Expected metadata object: {url}")
                return value
            except OSError:
                if attempt + 1 == self.attempts:
                    raise
                LOGGER.warning("Metadata request failed; retry %s/%s", attempt + 2, self.attempts)
                time.sleep(archive_http_delay(attempt))
        raise RuntimeError("Metadata retries exhausted")


@dataclass(frozen=True)
class ReleaseEvidence:
    release_date: str
    publisher: str
    checksums: tuple[tuple[str, str], ...]
    url: str
    revision: str
    content_fingerprint: str


def safe_member_path(name: str) -> PurePosixPath:
    """Validate a relative archive path and reject unsafe path components."""
    path = PurePosixPath(name)
    if not name or path.is_absolute() or "\\" in name or any(x in ("", ".", "..") for x in name.split("/")):
        raise ValueError(f"Unsafe archive path: {name!r}")
    if any(ord(x) < 32 for x in name) or ":" in name or name.startswith(("-", "@")):
        raise ValueError(f"Unsafe archive path: {name!r}")
    return path


def select_archive(metadata: JSONObject) -> JSONObject:
    """Select one original Stack Overflow archive with size and checksums."""
    files: list[JSONObject] = []
    for entry in json_array(metadata.get("files", [])):
        value = json_object(entry)
        name = json_string(value.get("name", ""))
        if PurePosixPath(name).name == "stackoverflow.com.7z":
            safe_member_path(name)
            if value.get("source") not in ("original", None) or value.get("format") in ("Directory", "Metadata"):
                raise ValueError("Stack Overflow archive must be a regular original file")
            files.append(value)
    if len(files) != 1:
        raise ValueError(f"Expected exactly one stackoverflow.com.7z; found {len(files)}")
    value = files[0]
    if _published_size(value.get("size", 0)) <= 0:
        raise ValueError("Archive has no positive published size")
    checksums: JSONObject = {}
    for algorithm, width in [("md5", 32), ("sha1", 40), ("sha256", 64)]:
        digest = value.get(algorithm)
        if isinstance(digest, str) and re.fullmatch(f"[0-9a-fA-F]{{{width}}}", digest):
            checksums[algorithm] = digest.lower()
    if not checksums:
        raise ValueError("Archive has no supported published checksum")
    return {**value, "checksums": checksums}


def parse_publisher_checksums(body: str) -> tuple[tuple[str, str], ...]:
    """Extract one unambiguous Stack Overflow SHA256 from publisher text."""
    text = unescape(re.sub(r"<[^>]+>", "\n", body))
    digests: set[str] = set()
    for line in text.splitlines():
        fields = line.split()
        if "stackoverflow.com.7z" in fields:
            digests.update(x.lower() for x in fields if re.fullmatch("[0-9a-fA-F]{64}", x))
    if len(digests) != 1:
        raise ValueError("Independent publisher evidence has no unambiguous Stack Overflow SHA256")
    return (("sha256", digests.pop()),)


class PublisherEvidence:
    """
    Read trusted Meta release evidence independently of IA metadata URLs.

    The checksum index links immutable official answer revisions. A release is
    usable only if its mirror link and corresponding publisher revision agree.
    """

    def __init__(self, client: MetadataClient) -> None:
        """Store the metadata client and initialize publisher evidence caches."""
        self.client = client
        self.records: dict[str, tuple[tuple[str, str], tuple[str, str]]] | None = None
        self.index_document: JSONObject = {}
        self.documents: dict[str, JSONObject] = {}

    def candidates(self) -> dict[str, str]:
        """Refresh the catalogue and fingerprint each date/revision reference."""
        url = "https://api.stackexchange.com/2.3/answers/402072?site=meta.stackexchange&filter=withbody"
        document = self.client.json(url)
        self.documents[url] = document
        items = _items(document)
        if len(items) != 1 or items[0].get("answer_id") != 402072:
            raise ValueError("Trusted release index unavailable")
        body = json_string(items[0]["body"])
        self.index_document = document
        self.records = {}
        for row_match in re.finditer(r"<tr\b[^>]*>(.*?)</tr>", body, re.S):
            row = row_match[1]
            archive = re.search(r"(?:https:)?//archive\.org/details/([A-Za-z0-9._-]+)", row)
            revision = re.search(r"https://meta\.stackexchange\.com/revisions/(\d+)/(\d+)", row)
            month_match = re.search(
                r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d\d)",
                unescape(re.sub("<[^>]*>", " ", row)),
            )
            if archive and revision and month_match:
                self.records[archive[1]] = ((month_match[1], month_match[2]), (revision[1], revision[2]))
        return {item_id: canonical_hash(record, ensure_ascii=False) for item_id, record in self.records.items()}

    def __call__(self, item_id: str) -> ReleaseEvidence | None:
        """Read immutable publisher release evidence for an item, or None."""
        if self.records is None:
            self.candidates()
        assert self.records is not None
        self.documents = {
            "https://api.stackexchange.com/2.3/answers/402072?site=meta.stackexchange&filter=withbody": self.index_document,
        }
        record = self.records.get(item_id)
        if not record:
            return None
        (month, year), (answer, revision_number) = record
        # Revision API is immutable; current answer bodies can contain a later release.
        url = f"https://api.stackexchange.com/2.3/posts/{answer}/revisions?site=meta.stackexchange&filter=withbody"
        answer_url = f"https://api.stackexchange.com/2.3/answers/{answer}?site=meta.stackexchange&filter=withbody"
        answer_document = self.client.json(answer_url)
        self.documents[answer_url] = answer_document
        answers = _items(answer_document)
        if len(answers) != 1 or str(answers[0].get("answer_id")) != answer or answers[0].get("question_id") != 402017:
            raise ValueError("Independent publisher post relationship could not be established")
        owner = json_object(answers[0].get("owner", {})).get("user_id")
        if not isinstance(owner, int):
            raise ValueError("Independent publisher account unavailable")
        user_url = f"https://api.stackexchange.com/2.3/users/{owner}?site=meta.stackexchange"
        user_document = self.client.json(user_url)
        self.documents[user_url] = user_document
        users = _items(user_document)
        if len(users) != 1 or users[0].get("user_id") != owner or users[0].get("is_employee") is not True:
            raise ValueError("Independent publisher account is not a verified Stack Exchange employee")
        document = self.client.json(url)
        self.documents[url] = document
        matching = [x for x in _items(document) if str(x.get("revision_number")) == revision_number and "body" in x]
        if len(matching) != 1:
            raise ValueError(f"Publisher revision unavailable for {item_id}")
        value = matching[0]
        if (
            str(value.get("post_id")) != answer
            or value.get("post_type") != "answer"
            or json_object(value.get("user", {})).get("user_id") != owner
        ):
            raise ValueError("Independent publisher revision was not authored by the verified publisher")
        months = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]
        release_date = f"{int(year):04d}-{months.index(month) + 1:02d}"
        evidence_url = f"https://meta.stackexchange.com/revisions/{answer}/{revision_number}"
        return ReleaseEvidence(
            release_date,
            "Stack Exchange",
            parse_publisher_checksums(json_string(value["body"])),
            evidence_url,
            json_string(value.get("revision_guid", revision_number)),
            sha256(json_string(value["body"]).encode()).hexdigest(),
        )


def _identity_order(identity: SnapshotIdentity) -> tuple[str, str]:
    """Sort snapshot identities by release date and then item ID."""
    return identity.release_date, identity.item_id


class ArchiveSource:
    def __init__(
        self,
        client: MetadataClient,
        evidence: Callable[[str], ReleaseEvidence | None] | None = None,
        page_rows: int = 100,
        *,
        cache_dir: Path | None = None,
        rejection_retry: timedelta = timedelta(hours=24),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """Set discovery dependencies, cache policy, and the evidence clock."""
        if rejection_retry.total_seconds() <= 0:
            raise ValueError("Discovery rejection retry interval must be positive")
        self.cache_dir, self.rejection_retry, self.clock = cache_dir, rejection_retry, clock
        self.client = client
        self.evidence = evidence or PublisherEvidence(client)
        self.page_rows = page_rows
        self.metadata: dict[str, JSONObject] = {}

    @staticmethod
    def download_url(identity: SnapshotIdentity) -> str:
        """Build the archive download URL with escaped item and member names."""
        return (
            f"https://archive.org/download/{quote(identity.item_id, safe='')}/{quote(identity.archive_name, safe='/')}"
        )

    def _identity(self, item_id: str) -> SnapshotIdentity:
        """Corroborate mirror metadata with independent publisher evidence."""
        validate_item_id(item_id)
        evidence = self.evidence(item_id)
        if evidence is None or not evidence.checksums:
            raise ValueError(f"{item_id}: no independent publisher evidence")
        metadata = self.client.json(f"https://archive.org/metadata/{quote(item_id, safe='')}")
        if json_object(metadata.get("metadata", {})).get("identifier") != item_id:
            raise ValueError("IA metadata item identity mismatch")
        archive = select_archive(metadata)
        date.fromisoformat(
            evidence.release_date + "-01"
            if re.fullmatch(r"\d{4}-\d{2}", evidence.release_date)
            else evidence.release_date,
        )
        hashes = json_object(archive["checksums"])
        for algorithm, digest in evidence.checksums:
            if algorithm in hashes and hashes[algorithm] != digest:
                raise ValueError(f"{item_id}: publisher {algorithm} conflicts with mirror")
            hashes[algorithm] = digest
        self.metadata = {item_id: metadata}
        return SnapshotIdentity(
            item_id,
            evidence.release_date,
            json_string(archive["name"]),
            _published_size(archive["size"]),
            tuple(sorted((algorithm, json_string(digest)) for algorithm, digest in hashes.items())),
            evidence.publisher,
            str(json_object(metadata["metadata"]).get("uploader", "unknown")),
            (
                ("url", evidence.url),
                ("revision", evidence.revision),
                ("fingerprint", evidence.content_fingerprint),
                ("verification", "publisher checksum corroborated"),
            ),
        )

    def resolve(self, requested_snapshot: str) -> SnapshotIdentity:
        """Resolve a requested snapshot or the latest corroborated release."""
        LOGGER.info("Resolve snapshot %s using independent publisher evidence", requested_snapshot)
        if requested_snapshot != "latest":
            identity = self._identity(requested_snapshot)
            LOGGER.info(
                "Selected %s; release %s; archive %s",
                identity.item_id,
                identity.release_date,
                format_bytes(identity.size),
            )
            return identity
        if isinstance(self.evidence, PublisherEvidence):
            return self._resolve_catalogue(self.evidence)
        best: SnapshotIdentity | None = None
        best_metadata: dict[str, JSONObject] = {}
        best_documents: dict[str, JSONObject] = {}
        seen: set[str] = set()
        reasons: list[str] = []
        queries = [
            'title:("Stack Exchange" OR "StackExchange" OR "Stack Overflow") AND mediatype:data',
            "identifier:stackexchange* OR identifier:stack-exchange* OR identifier:stackoverflow*",
        ]
        for query_index, query in enumerate(queries, start=1):
            with ProgressReporter(
                LOGGER, f"Snapshot search {query_index}/{len(queries)}", unit="items"
            ).track() as progress:
                page = 1
                inspected = 0
                while True:
                    parameters = urlencode(
                        {"q": query, "fl[]": "identifier", "rows": self.page_rows, "page": page, "output": "json"},
                    )
                    response = json_object(
                        self.client.json(f"https://archive.org/advancedsearch.php?{parameters}")["response"],
                    )
                    LOGGER.debug("Snapshot search page %s: %s matches", page, response["numFound"])
                    total = json_integer(response["numFound"])
                    progress.update(inspected, total)
                    for entry in json_array(response["docs"]):
                        value = json_object(entry)
                        item_id = json_string(value["identifier"])
                        if item_id in seen:
                            inspected += 1
                            progress.update(inspected, total)
                            continue
                        seen.add(item_id)
                        try:
                            candidate = self._identity(item_id)
                            if best is None or (candidate.release_date, candidate.item_id) > (
                                best.release_date,
                                best.item_id,
                            ):
                                best, best_metadata = candidate, self.metadata
                                best_documents = (
                                    dict(self.evidence.documents)
                                    if isinstance(self.evidence, PublisherEvidence)
                                    else {}
                                )
                                LOGGER.debug(
                                    "Newest eligible candidate: %s; release %s", best.item_id, best.release_date
                                )
                        except (ValueError, KeyError) as error:
                            reasons.append(f"{item_id}: {error}")
                            LOGGER.debug("Reject snapshot %s: %s", item_id, error)
                        inspected += 1
                        progress.update(inspected, total)
                    if page * self.page_rows >= json_integer(response["numFound"]):
                        progress.update(inspected, total, force=True)
                        break
                    page += 1
        if best is None:
            raise ValueError("No independently verified snapshot: " + "; ".join(reasons))
        self.metadata = best_metadata
        if isinstance(self.evidence, PublisherEvidence):
            self.evidence.documents = best_documents
        LOGGER.info("Selected %s; release %s; archive %s", best.item_id, best.release_date, format_bytes(best.size))
        return best

    def verify_recorded(self, identity: SnapshotIdentity) -> None:
        """Reject a snapshot whose current remote identity differs from the record."""
        current = self._identity(identity.item_id)
        if current.digest != identity.digest:
            raise ValueError(f"Remote source identity changed: {identity.item_id}")

    def _resolve_catalogue(self, evidence: PublisherEvidence) -> SnapshotIdentity:
        """Resolve the newest publisher-backed identity using a locked cache."""
        if self.cache_dir is not None and self.cache_dir.is_symlink():
            raise ValueError("Discovery cache directory must not be a symlink")
        lock = SnapshotLock(self.cache_dir) if self.cache_dir is not None else nullcontext()
        with lock:
            references = evidence.candidates()
            cache = DiscoveryCache(self.cache_dir) if self.cache_dir is not None else None
            if cache is not None and cache.store.path.is_symlink():
                raise ValueError("Discovery cache file must not be a symlink")
            try:
                entries = cache.read() if cache is not None else {}
            except (ValueError, KeyError, TypeError) as error:
                LOGGER.warning("Invalid discovery cache; rebuild from publisher catalogue: %s", error)
                entries = {}
            entries = {item_id: entry for item_id, entry in entries.items() if item_id in references}
            now = self.clock()
            if now.tzinfo is None:
                raise ValueError("Discovery clock must include a timezone")
            pending = [
                item_id
                for item_id, reference in references.items()
                if (entry := entries.get(item_id)) is None
                or entry.reference != reference
                or entry.checked_at > now
                or (entry.identity is None and now - entry.checked_at >= self.rejection_retry)
            ]
            LOGGER.info(
                "Snapshot catalogue: %s entries; %s cached; %s new, changed or due for retry",
                len(references),
                len(references) - len(pending),
                len(pending),
            )
            if pending:
                with ProgressReporter(
                    LOGGER, "Snapshot catalogue", unit="items", total=len(pending)
                ).track() as progress:
                    for completed, item_id in enumerate(pending, start=1):
                        try:
                            identity = self._identity(item_id)
                            entries[item_id] = DiscoveryEntry(references[item_id], now, identity)
                        except (ValueError, KeyError) as error:
                            entries[item_id] = DiscoveryEntry(references[item_id], now, None, str(error))
                            LOGGER.debug("Reject snapshot %s: %s", item_id, error)
                        if cache is not None:
                            cache.write(entries, now)
                        progress.update(completed)
                    progress.update(len(pending), force=True)
            # Cached identities rank candidates; only fresh evidence may select one.
            for _ in range(2 * len(references) + 1):
                identities = [entry.identity for entry in entries.values() if entry.identity is not None]
                if not identities:
                    reasons = "; ".join(f"{item_id}: {entry.rejection}" for item_id, entry in entries.items())
                    raise ValueError("No independently verified snapshot: " + reasons)
                ranked = max(identities, key=_identity_order)
                try:
                    fresh = (
                        ranked
                        if ranked.item_id in self.metadata and ranked.item_id in pending
                        else self._identity(ranked.item_id)
                    )
                    entries[ranked.item_id] = DiscoveryEntry(references[ranked.item_id], now, fresh)
                except (ValueError, KeyError) as error:
                    entries[ranked.item_id] = DiscoveryEntry(references[ranked.item_id], now, None, str(error))
                    if cache is not None:
                        cache.write(entries, now)
                    continue
                if cache is not None:
                    cache.write(entries, now)
                if fresh.digest != ranked.digest:
                    continue
                LOGGER.info(
                    "Selected %s; release %s; archive %s", fresh.item_id, fresh.release_date, format_bytes(fresh.size)
                )
                return fresh
            raise RuntimeError("Snapshot identities changed repeatedly during discovery; retry later")
