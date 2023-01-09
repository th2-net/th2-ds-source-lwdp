from typing import Dict


class Page:
    def __init__(self, data: Dict):  # noqa
        """Page Constructor.

        Args:
            data (Dict): Page Data
        """
        id_ = data.pop("id")
        data["book"] = id_["book"]
        data["name"] = id_["name"]
        self.data = data
        self.book = data["book"]
        self.name = data["name"]
        self.comment = data["comment"]
        self.start_timestamp = data["started"]
        self.end_timestamp = data["ended"]
        self.updated = data["updated"]
        self.removed = data["removed"]

    def __str__(self):  # noqa
        return str(self.data)

    def __repr__(self):  # noqa
        return self.__str__()


# class PageNotFound(Exception):
#     def __init__(self, book_id):
#         """Exception for the case when the page was not found in data source.
#
#         Args:
#             book_id: Book id.
#
#         """
#         self._book_id = book_id
#
#     def __str__(self):
#         return f"Unable to find the page with book id '{self.book_id}'"
