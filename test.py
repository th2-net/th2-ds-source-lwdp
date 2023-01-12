
from datetime import datetime, timezone, timedelta
from th2_data_services_lwdp.data_source import HTTPDataSource

from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.filters.filter import LwDPFilter

ds_api = HTTPAPI("http://th2-kuber-test03:32681")

START_TIME = datetime(year=2022, month=11, day=10, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=53, second=8, microsecond=0)
st = int(START_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)
et = int(END_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)

# url = ds.command(
#     commands.GetMessagesByBookByStreams(
#         start_timestamp=st, end_timestamp=et, streams=streams, book_id="case3"
#     )
# )
# for i in url:
#     print(i)

# # Will Return `408 Status Code` And MessageNotFound Will Be Raised
#message = ds.command(
#    commands.GetEventById("demo_book_1:th2-scope:20230105135705560873000:d61e930a-8d00-11ed-aa1a-d34a6155152d_1")
#)
#print(message)
#pages = ds.command(commands.GetPages("demo_book_1", start_timestamp=START_TIME, end_timestamp=END_TIME))

#messages = ds.command(
 #   commands.GetMessagesByPageByGroups(page=list(pages)[0],groups=['ds-lib-session1','ds-lib-session2'])
#)

#print(len(list(messages)))
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.commands import http as commands

ds = HTTPDataSource("http://th2-kuber-test03:32681")

START_TIME = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=5, microsecond=0) 
END_TIME   = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=6, microsecond=0)  
BOOK_NAME='demo_book_1'
STREAMS = ['ds-lib-session1','ds-lib-session2']

messages=ds.command(
    commands.GetMessagesByBookByStreams(start_timestamp=START_TIME, end_timestamp=END_TIME, book_id=BOOK_NAME,streams=STREAMS)
)
print(messages)

#page = list(pages)[0]
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