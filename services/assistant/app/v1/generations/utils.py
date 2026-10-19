"""The ``BulkJobStatus`` transition table (§3.1.1.3) and terminal-status arithmetic. No I/O."""

from app.v1.generations.enums import BulkJobStatus

B = BulkJobStatus
TERMINAL = frozenset({B.COMPLETED, B.PARTIALLY_FAILED, B.FAILED, B.CANCELLED})
TRANSITIONS: dict[BulkJobStatus, frozenset[BulkJobStatus]] = {
    B.QUEUED: frozenset({B.RUNNING, B.CANCELLED}),
    B.RUNNING: frozenset({B.COMPLETED, B.PARTIALLY_FAILED, B.FAILED, B.CANCELLED}),
    B.COMPLETED: frozenset(),
    B.PARTIALLY_FAILED: frozenset(),
    B.FAILED: frozenset(),
    B.CANCELLED: frozenset(),
}


class IllegalJobTransition(ValueError):
    def __init__(self, current: BulkJobStatus, target: BulkJobStatus) -> None:
        super().__init__(f"bulk job cannot move from {current} to {target}")


def check_transition(current: BulkJobStatus, target: BulkJobStatus) -> None:
    if target not in TRANSITIONS[current]:
        raise IllegalJobTransition(current, target)


def final_status(completed: int, failed: int) -> BulkJobStatus:
    """The terminal status once every item has been processed."""
    if failed == 0:
        return B.COMPLETED
    if completed == 0:
        return B.FAILED
    return B.PARTIALLY_FAILED
