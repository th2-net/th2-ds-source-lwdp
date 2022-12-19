from typing import Dict, Union
from . import seconds2ms


class Page:
    def __init__(self, data: Dict):  # noqa
        """Page Constructor.

        Args:
            data (Dict): Page Data
        """
        id_ = data.pop("id")
        data["book"] = id_["book"]
        data["name"] = id_["name"]
        self.__data = data

    @property
    def data(self) -> Dict:
        """Object As Dict.

        Returns:
            Dict
        """
        return self.__data

    @property
    def start_timestamp(self) -> int:
        """Get Start Timestamp In ms.

        Returns:
            int
        """
        return seconds2ms(self.data["started"]["epochSecond"])

    @property
    def end_timestamp(self) -> Union[int, None]:
        """Get End Timestamp In ms.

        Returns:
            Union[int, None]
        """
        return None if self.data["ended"] is None else seconds2ms(self.data["ended"]["epochSecond"])

    @property
    def book_id(self) -> str:
        """Get Book ID.

        Returns:
            str
        """
        return self.data["book"]

    @property
    def book_name(self) -> str:
        """Get Book Name.

        Returns:
            str
        """
        return self.data["name"]

    @property
    def comment(self) -> Union[None, str]:
        """Get Book Comment.

        Returns:
            Union[None, str]
        """
        return self.data["comment"]

    @property
    def updated(self) -> Union[int, None]:
        """Get Update Timestamp In ms.

        Returns:
            Union[int, None]
        """
        return (
            None
            if self.data["updated"] is None
            else seconds2ms(self.data["updated"]["epochSecond"])
        )

    @property
    def removed(self) -> Union[int, None]:
        """Get Remove Timestamp In ms.

        Returns:
            Union[int, None]
        """
        return (
            None
            if self.data["removed"] is None
            else seconds2ms(self.data["removed"]["epochSecond"])
        )

    def __str__(self):  # noqa
        return str(self.data)

    def __repr__(self):  # noqa
        return self.__str__()
