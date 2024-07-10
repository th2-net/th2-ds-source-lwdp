import pytest
from unittest.mock import Mock, patch, MagicMock

from th2_data_services.data import Data
from th2_data_services.utils.converters import ProtobufTimestampConverter

from th2_data_services.data_source.lwdp.commands.http import GetMessagesByPageByGroups2
from th2_data_services.data_source.lwdp.data_source import DataSource


@pytest.fixture
def mock_data_source():
    mock = Mock(spec=DataSource)
    mock.source_api = MagicMock()
    mock.source_api.post_download_messages.return_value = ("test_url", "test_body")
    return mock


def test_get_messages_by_pages_by_groups2_handle(mock_data_source):
    command = GetMessagesByPageByGroups2("test_page", ["group1"], "test_book")

    with patch('th2_data_services.data_source.lwdp.commands.http._get_page_object'):
        with patch.object(ProtobufTimestampConverter, 'to_nanoseconds'):
            with patch('th2_data_services.data_source.lwdp.commands.http.get_utc_datetime_now'):
                with patch('th2_data_services.data_source.lwdp.commands.http._download_messages') as mock_download:
                    expected = [{"message": "test"}, {"message1": "test"}, {"message2": "test"}]
                    mock_download.return_value = iter([{'status': 'success'}, *expected])

                    result = command.handle(mock_data_source)
                    i = 0
                    for message in result:
                        assert message == expected[i]
                        i += 1

                    assert isinstance(result, Data)


if __name__ == "__main__":
    pytest.main()
