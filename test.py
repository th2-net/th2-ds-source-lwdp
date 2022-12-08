from datetime import datetime, timezone
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.source_api.grpc import GRPCAPI
from th2_data_services_lwdp.commands import http as commands

ds_api = HTTPAPI("http://th2-kuber-test03:32681")
ds = HTTPDataSource("http://10.100.66.105:32681")

START_TIME = datetime(year=2022, month=11, day=10, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=10, hour=13, minute=53, second=8, microsecond=0)
# st = int(START_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)
# et = int(END_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)


events = ds.command(commands.GetEvents(
    start_timestamp=START_TIME, end_timestamp=END_TIME, book_id='case3', scope='th2-scope'))

# for i in events:
#     print(i)

START_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=53, second=8, microsecond=0)

msgs = ds.command(commands.GetMessages(
    start_timestamp=START_TIME,
    end_timestamp=END_TIME,
    book_id='case3',
    streams=['arfq02fix30']))

# print(msgs.len)

START_TIME = datetime(year=2022, month=11, day=11, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=20, minute=53, second=8, microsecond=0)

messages_by_groups = ds.command(
    commands.GetMessagesByGroups(
        start_timestamp=START_TIME,
        end_timestamp=END_TIME,
        book_id="case3",
        groups=["Case030", "Case031", "Case032", "arfq02dc30", "arfq02fix30", "csvtest1"]))
for i in messages_by_groups:
    print(i)
