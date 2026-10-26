"""Track temporary bytes and check free disk space before artifact writes."""

import json
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, TextIO

from utils.artifacts import atomic_write_json
from utils.resources import DiskBudget
from utils.types import JSONObject


class ArtifactBudget(DiskBudget):
    def write_bytes(self, stream: BinaryIO, directory: Path, data: bytes) -> int:
        """Check disk space and write bytes to the supplied stream."""
        self.require(directory, len(data))
        return stream.write(data)

    def write_json(self, path: Path, value: JSONObject) -> None:
        # JSON contains only bounded metadata, and reserve covers formatting overhead.
        """Reserve metadata space and atomically write a JSON object."""
        self.require(path.parent, len(json.dumps(value).encode("utf-8")) + 65536)
        atomic_write_json(path, value)

    @contextmanager
    def open_text(self, path: Path) -> Iterator[BudgetTextWriter]:
        """Provide a UTF-8 text writer that accounts for spill bytes."""
        with path.open("w", encoding="utf-8", newline="\n") as stream:
            yield BudgetTextWriter(self, path, stream)


class BudgetTextWriter:
    def __init__(self, budget: ArtifactBudget, path: Path, stream: TextIO) -> None:
        """Bind a text stream and its destination to an artifact budget."""
        self.budget, self.path, self.stream = budget, path, stream

    def write(self, text: str) -> int:
        """Charge encoded text bytes to the spill budget before writing."""
        self.budget.require(self.path.parent, len(text.encode("utf-8")), spill=True)
        return self.stream.write(text)

    def writelines(self, lines: Iterable[str]) -> None:
        """Write each line through the same spill-budget check."""
        for line in lines:
            self.write(line)
