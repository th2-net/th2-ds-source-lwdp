from datetime import datetime, timezone

from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.commands import http as commands

ds_api = HTTPAPI("http://th2-kuber-test03:32681")
ds = HTTPDataSource("http://10.100.66.105:32681")

START_TIME = datetime(year=2022, month=11, day=10, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=53, second=8, microsecond=0)
st = int(START_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)
et = int(END_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)

streams = [
    "Test-1234",
    "Test-1234",
    "Test-12345",
    "Test-123456",
    "Test-1234567",
    "Test-12345678",
    "Test-123456789",
    "Test-1234567810",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest1",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest2",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest3",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest4",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest5",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest6",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest7",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest8",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest9",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest10",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest11",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest12",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest13",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest14",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest15",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest16",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest17",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest18",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest19",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest20",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest21",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest22",
    "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest23",
    "arfq01fix07",
    "arfq01dc03",
    "arfq02dc10",
    "arfq02fix30",
]

# url = ds.command(
#     commands.GetMessagesByBookByStreams(
#         start_timestamp=st, end_timestamp=et, streams=streams, book_id="case3"
#     )
# )
# for i in url:
#     print(i)

# # Will Return `408 Status Code` And MessageNotFound Will Be Raised
message = ds.command(
    commands.GetMessageById("case3:arfq02dc30:2:20221111165020871966000:1668182270286678628")
)
print(message)


pages = ds.command(commands.GetPages("case3", datetime.fromtimestamp(1668013240), datetime.now()))
print(pages)

page = list(pages)[0]
# s = time.time()
# # Will Return `error` Event
# messages = ds.command(
#     commands.GetMessagesByPageByStreams(
#         page, ["arfq02dc30"]
#     )
# )
# print(messages)
# print(time.time() - s)

# page = list(pages)[0] 
# s = time.time()
# messages = ds.command(
#     commands.GetMessagesByPageByGroups(
#         page, ["group1", "group2"]
#     )
# )
# print(messages)
# print(time.time() - s)


# messages = ds.command(commands.GetEventsByPageByScopes(page, ["th2-scope"]))
# print(messages)