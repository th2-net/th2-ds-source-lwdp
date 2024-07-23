import pytest
from unittest.mock import Mock, patch, MagicMock

import orjson
from th2_data_services.data import Data

from th2_data_services.data_source.lwdp.commands.http import (
    GetMessagesByPageByGroupsJson,
    GetMessagesByBookByGroupsJson,
    IterStatus,
)
from th2_data_services.data_source.lwdp.data_source import DataSource


@pytest.fixture
def mock_source_api():
    mock_api = MagicMock()
    mock_api.post_download_messages.return_value = ("test_url", "test_body")
    return mock_api


@pytest.fixture
def mock_execute_post_response():
    mock_response = MagicMock()
    mock_response.text = orjson.dumps({"taskID": "test_task_id"}).decode()
    return mock_response


@pytest.fixture
def mock_messages_response():
    mock_response = MagicMock()

    def iter_lines():
        yield orjson.dumps({"message": "test"})
        yield orjson.dumps({"message1": "test"})
        yield orjson.dumps({"message2": "test"})

    mock_response.iter_lines.return_value = iter_lines()
    return mock_response


@pytest.fixture
def mock_status_response():
    mock_response = MagicMock()
    mock_response.text = orjson.dumps({"taskID": "a", "createdAt": "b", "completedAt": "c", "status": "success"}).decode()
    return mock_response


@pytest.fixture
def mock_data_source(
    mock_source_api, mock_execute_post_response, mock_messages_response, mock_status_response
):
    mock = Mock(spec=DataSource)
    mock.source_api = mock_source_api
    mock.source_api.execute_post.return_value = mock_execute_post_response
    mock.source_api.execute_request.side_effect = [
        mock_messages_response,
        mock_status_response,
        mock_messages_response,
        mock_status_response,
    ]
    return mock


def test_get_messages_by_pages_by_groups2_handle(mock_data_source):
    command = GetMessagesByPageByGroupsJson("test_page", ["group1"], "test_book")

    with patch("th2_data_services.data_source.lwdp.commands.http._get_page_object"):
        expected = [{"message": "test"}, {"message1": "test"}, {"message2": "test"}]

        result = command.handle(mock_data_source)
        i = 0
        for message in result:
            assert message == expected[i]
            i += 1

        expected_status = IterStatus(**{"taskID": "a", "createdAt": "b", "completedAt": "c", "status": "success"})
        assert isinstance(result, Data)
        assert result.metadata == {'iter_statuses': expected_status}

        i = 0
        for message in result:
            assert message == expected[i]
            i += 1


def test_get_messages_by_book_by_groups2_handle(mock_data_source):
    command = GetMessagesByBookByGroupsJson(
        1718355600000000000, 1718362800000000000, book_id="test_book", groups=["group1", "group2"]
    )

    expected = [{"message": "test"}, {"message1": "test"}, {"message2": "test"}]

    result = command.handle(mock_data_source)
    i = 0
    for message in result:
        assert message == expected[i]
        i += 1

    expected_status = IterStatus(**{"taskID": "a", "createdAt": "b", "completedAt": "c", "status": "success"})
    assert isinstance(result, Data)
    assert result.metadata == {'iter_statuses': expected_status}

    i = 0
    for message in result:
        assert message == expected[i]
        i += 1
