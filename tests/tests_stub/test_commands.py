import pytest
from unittest.mock import Mock, patch, MagicMock

import orjson
from th2_data_services.data import Data

from th2_data_services.data_source.lwdp.commands.http import GetMessagesByPageByGroups2
from th2_data_services.data_source.lwdp.data_source import DataSource


@pytest.fixture
def mock_data_source():
    mock = Mock(spec=DataSource)
    mock.source_api = MagicMock()
    mock.source_api.post_download_messages.return_value = ("test_url", "test_body")

    mock_response = MagicMock()
    mock_response.text = orjson.dumps({"taskID": "test_task_id"}).decode()
    mock.source_api.execute_post.return_value = mock_response
    mock_messages_response = MagicMock()
    mock_messages_response.iter_lines.return_value = [
        orjson.dumps({"message": "test"}),
        orjson.dumps({"message1": "test"}),
        orjson.dumps({"message2": "test"})
    ]
    mock.source_api.execute_request.side_effect = [
        mock_messages_response,
        MagicMock(text=orjson.dumps({"status": "success"}).decode())
    ]
    return mock


def test_get_messages_by_pages_by_groups2_handle(mock_data_source):
    command = GetMessagesByPageByGroups2("test_page", ["group1"], "test_book")

    with patch('th2_data_services.data_source.lwdp.commands.http._get_page_object'):
        expected = [{"message": "test"}, {"message1": "test"}, {"message2": "test"}]

        result = command.handle(mock_data_source)
        i = 0
        for message in result:
            assert message == expected[i]
            i += 1

        assert isinstance(result, Data)
        assert result.metadata == {"status": "success"}

        i = 0
        for message in result:
            assert message == expected[i]
            i += 1


if __name__ == "__main__":
    pytest.main()
