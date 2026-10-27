"""A stand-in for Harlequin's driver that records what interactions hand it."""

from __future__ import annotations

from typing import Any, Callable


class RecordingDriver:
    """Records inserted text and notifications; confirms every prompt."""

    def __init__(self) -> None:
        self.inserted: list[str] = []
        self.notifications: list[tuple[str, str]] = []

    def insert_text_in_new_buffer(self, text: str) -> None:
        self.inserted.append(text)

    def insert_text_at_selection(self, text: str) -> None:
        self.inserted.append(text)

    def notify(self, message: str, severity: str = "information") -> None:
        self.notifications.append((message, severity))

    def confirm_and_execute(self, callback: Callable[[], None]) -> None:
        callback()

    def refresh_catalog(self) -> None:
        pass


def run_interaction(
    interaction: Callable[[Any, Any], None], item: object, driver: RecordingDriver
) -> None:
    """Runs `interaction` the way Harlequin would, with the recording driver."""
    interaction(item, driver)
