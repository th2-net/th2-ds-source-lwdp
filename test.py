from datetime import datetime, timezone
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.commands import http as commands

# ds = HTTPDataSource("http://th2-kuber-test03:30452")
ds = HTTPDataSource("http://10.100.66.105:32681")

START_TIME = datetime(year=2022, month=11, day=10, hour=12, minute=0, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=10, hour=13, minute=53, second=8, microsecond=0)
st = int(START_TIME.replace(tzinfo=timezone.utc).timestamp() * 10 ** 9)
et = int(END_TIME.replace(tzinfo=timezone.utc).timestamp() * 10 ** 9)

# events = ds.command(commands.GetEvents(
#     start_timestamp=START_TIME, end_timestamp=END_TIME))

events = ds.command(commands.GetEvents(
    start_timestamp=START_TIME, end_timestamp=END_TIME, book_id='case3', scope='th2-scope'))

for i in events:
    print(i)

# START_TIME = datetime(year=2022, month=11, day=11, hour=12, minute=0, second=0, microsecond=0)
# END_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=53, second=8, microsecond=0)
#
# msgs = ds.command(commands.GetMessages(
#     start_timestamp=START_TIME,
#     end_timestamp=END_TIME,
#     book_id='case3',
#     stream=['arfq02fix30']))
#
# for i in msgs:
#     print(i)
