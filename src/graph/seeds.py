"""Resolve vector question IDs using snapshot, registry, and graph membership."""

import re
from dataclasses import dataclass
from typing import Protocol

from graph.adapter.sparql import SelectResult
from graph.domain.config import RunIdentity
from graph.interfaces.queries import SelectClient
from graph.mapping.identity import NAMESPACE, IriPolicy
from vector.domain.models import PointPayload


@dataclass(frozen=True)
class VectorSeed:
    question_id: int
    snapshot_digest: str
    registry_digest: str


class QuestionMembership(Protocol):
    def membership(self, question_id: int) -> tuple[str, ...] | None:
        """Read a question's graph memberships, or return None if it is absent."""
        ...


class SeedOutsideLoadedGraphsError(LookupError):
    pass


class SeedSourceMissingError(LookupError):
    pass


def seed_from_payload(payload: PointPayload, vector_registry_digest: str) -> VectorSeed:
    """Build a source-bound vector seed from validated IDs and digests."""
    snapshot = payload["snapshot_id"]
    if (
        payload["question_id"] <= 0
        or re.fullmatch(r"[0-9a-f]{64}", snapshot) is None
        or re.fullmatch(r"[0-9a-f]{64}", vector_registry_digest) is None
    ):
        raise ValueError("Vector seed requires a positive original ID and explicit source/registry digests")
    return VectorSeed(payload["question_id"], snapshot, vector_registry_digest)


class SeedLookup:
    def __init__(
        self,
        client: SelectClient,
        identity: RunIdentity,
        graph_keys: tuple[str, ...],
        *,
        membership: QuestionMembership,
    ) -> None:
        """Bind the query client, loaded graph scope, and source membership reader."""
        self.client, self.identity, self.graph_keys, self.membership = client, identity, graph_keys, membership

    def lookup(self, seed: VectorSeed) -> SelectResult:
        """
        Validate seed provenance and query its question in loaded member graphs.

        Args:
            seed: Original question ID with source snapshot and registry digests.

        Returns:
            Bounded question, answer, and comment bindings from loaded graphs.

        Raises:
            SeedSourceMissingError: The pinned source does not contain the question.
            SeedOutsideLoadedGraphsError: None of its member graphs is loaded.
            ValueError: Provenance differs or the loaded index lacks the question.

        """
        if (
            seed.snapshot_digest != self.identity.snapshot_digest
            or seed.registry_digest != self.identity.registry_digest
        ):
            raise ValueError("Vector seed belongs to a different snapshot or registry")
        graphs = self.membership.membership(seed.question_id)
        if graphs is None:
            raise SeedSourceMissingError(f"Question {seed.question_id} is absent from the pinned source")
        selected = tuple(key for key in graphs if key in self.graph_keys)
        if not selected:
            raise SeedOutsideLoadedGraphsError(f"Question {seed.question_id} belongs to unloaded graphs: {graphs}")
        iris = IriPolicy()
        graph_values = " ".join(f"<{iris.graph(key)}>" for key in selected)
        subject = iris.question(seed.question_id)
        query = f"""SELECT DISTINCT ?question_id ?answer ?accepted ?comment WHERE {{
 VALUES ?g {{ {graph_values} }} GRAPH ?g {{
 <{subject}> <{NAMESPACE}sourcePostId> ?question_id ; a <{NAMESPACE}Question> .
 OPTIONAL {{ <{subject}> <{NAMESPACE}hasAnswer> ?answer . OPTIONAL {{ ?answer <{NAMESPACE}hasComment> ?comment }} }}
 OPTIONAL {{ <{subject}> <{NAMESPACE}acceptedAnswer> ?accepted }}
 }} }} LIMIT 1000"""
        result = self.client.select(query)
        if not result.rows:
            raise ValueError("Index is missing a source question that belongs to its loaded graphs")
        return result
