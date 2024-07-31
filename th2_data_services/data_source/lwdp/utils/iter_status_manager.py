from dataclasses import dataclass


@dataclass
class IterStatus:
    taskID: str = None
    createdAt: str = None
    completedAt: str = None
    status: str = None
    errors: str = None


class StatusUpdateManager:
    def __init__(self, data):
        """Initialize the StatusUpdateManager.

        Args:
            data: The data object to manage status updates for.
        """
        self.__data = data

    def update(self, status):
        self.__data.update_metadata({"Iter statuses": IterStatus(**status)})
