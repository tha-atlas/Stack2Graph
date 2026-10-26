"""Retain source-record error details across reader worker processes."""


class SnapshotRecordBudgetError(ValueError):
    def __init__(self, table: str, source_id: int, field: str) -> None:
        """Record the table, source ID, and field that exceeded the byte budget."""
        self.table, self.source_id, self.field = table, source_id, field
        super().__init__(f"{table}[{source_id}].{field}: snapshot record exceeds configured record byte budget")

    def __reduce__(self) -> tuple[type[SnapshotRecordBudgetError], tuple[str, int, str]]:
        """Preserve error details when serializing across worker processes."""
        return type(self), (self.table, self.source_id, self.field)
