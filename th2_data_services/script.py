from datetime import datetime

from th2_data_services.data_source.lwdp.commands.http import DownloadMessagesByBookByGroupsGzip
from th2_data_services.data_source.lwdp.data_source import DataSource

START_TIME = datetime(year=2024, month=6, day=14, hour=9, minute=0, second=0, microsecond=0)
END_TIME = datetime(year=2024, month=6, day=14, hour=11, minute=0, second=0, microsecond=0)

data_source = DataSource("http://10.99.17.21:8082/")
result = data_source.command(DownloadMessagesByBookByGroupsGzip("f", start_timestamp=START_TIME,
                                                           end_timestamp=END_TIME, book_id="test_book", groups=["fixalias16", "fixalias11"]))
for message in result:
    print(message)
