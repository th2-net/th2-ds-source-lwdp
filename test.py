import time
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

# message = ds.command(
#     commands.GetMessageById(
#         "case3:arfq02dc30:2:20221111165020871966000:1668182270286678628"
#     )
# )
# {"timestamp":{"epochSecond":1668185420,"nano":871966000},"direction":"OUT","sessionId":"arfq02dc30","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"OD1GSVhULjEuMQE5PTkxATM1PUEBMzQ9MQE0OT1BUkZRMDJEQzMwATUyPTIwMjIxMTExLTE2OjUwOjIwLjg2NwE1Nj1GR1cBOTg9MAExMDg9NQExNDE9WQE1NTQ9bWl0MTIzATExMzc9OQExMD0xNjMB","messageId":"case3:arfq02dc30:2:20221111165020871966000:1668182270286678628"}
# {'id': 'case3:arfq02dc30:2:20221111165020871966000:1668182270286678628', 'error': "Operation hasn't done during timeout 60000 MILLISECONDS"}
# print(message)

pages = ds.command(
    commands.GetPages(
        "case3",
        datetime.fromtimestamp(1668013240),
        datetime.now()
    )
)
print(pages)

# page = list(pages)[0]
# s = time.time()
# messages = ds.command(
#     commands.GetMessagesByPageByStreams(
#         page, ["arfq02dc30"]
#     )
# )
# print(messages)
# print(time.time() - s)

'''
http://10.100.66.105:32681/search/sse/messages?startTimestamp=1668182473000&searchDirection=next&endTimestamp=1668182483000&bookId=case3&stream=arfq02dc30
------------- Printed first 5 records -------------
{'attachedEventIds': [],
 'body': {},
 'bodyBase64': 'OD1GSVhULjEuMQE5PTkxATM1PUEBMzQ9MQE0OT1BUkZRMDJEQzMwATUyPTIwMjIxMTExLTE2OjAxOjE4Ljg2OAE1Nj1GR1cBOTg9MAExMDg9NQExNDE9WQE1NTQ9bWl0MTIzATExMzc9OQExMD0xNjcB',
 'direction': 'OUT',
 'messageId': 'case3:arfq02dc30:2:20221111160118883679000:1668182270286678040',
 'messageType': '',
 'sessionId': 'arfq02dc30',
 'timestamp': {'epochSecond': 1668182478, 'nano': 883679000}}
{'attachedEventIds': [],
 'body': {},
 'bodyBase64': 'OD1GSVhULjEuMQE5PTkxATM1PUEBMzQ9MQE0OT1BUkZRMDJEQzMwATUyPTIwMjIxMTExLTE2OjAxOjEzLjg2OAE1Nj1GR1cBOTg9MAExMDg9NQExNDE9WQE1NTQ9bWl0MTIzATExMzc9OQExMD0xNjIB',
 'direction': 'OUT',
 'messageId': 'case3:arfq02dc30:2:20221111160113879497000:1668182270286678039',
 'messageType': '',
 'sessionId': 'arfq02dc30',
 'timestamp': {'epochSecond': 1668182473, 'nano': 879497000}}

60.05070209503174
'''

# page = list(pages)[0]
# s = time.time()
# messages = ds.command(
#     commands.GetMessagesByPageByGroups(
#         page, ["group1", "group2"]
#     )
# )
# print(messages)
# print(time.time() - s)

# messages = ds.command(
#     commands.GetEventsByPageByScopes(
#         page,
#         ["th2-scope"]
#     )
# )
# print(messages)