from typing import List, Union

class ResponseFormats:
    def __init__(self):  # noqa
        """ResponseFormats Constructor.
        """
        self.correct_formats = ["PROTO_PARSED","JSON_PARSED","BASE_64"]

    def is_valid_response_format(self, formats: Union[str,List[str]]):
        if isinstance(formats,str):
            formats = [formats]
        if not isinstance(formats,list):
            raise Exception("Wrong type. formats should be list or string")
        return all(format in self.correct_formats for format in formats)
