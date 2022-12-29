# Necessary imports for demo.
from th2_data_services_lwdp.commands import http as commands
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services import Data
from datetime import datetime

# Create grpc data source object to connect to lightweight data provider.
DEMO_HOST = "10.100.66.105"  # Note that this host is only accessible from exactpro domain
DEMO_PORT = "32681"
data_source = HTTPDataSource(f"http://{DEMO_HOST}:{DEMO_PORT}")

# On database data is segregated with books, such as they never intersect.
# To get the names of the books we have a command GetBooks which takes no argument:

books = data_source.command(commands.GetBooks())
book_id = "demo_book_1"  # demo_book_1 is an example book from host namespace
scopes = ["th2-scope"]
streams = [
    "default-message-producer-alias",
    "fix-demo-server1",
    "fix-demo-server2",
    "fix-client2",
    "fix-client1",
]
groups = streams  # In this namespace groups and streams have same name.

# In books data is partitioned even more.
# Events are grouped by scopes, which we can get using GetScopes command:

book_scopes = data_source.command(commands.GetEventScopes(book_id))

# Messages are separated by groups and aliases.
# To get groups we use GetMessageGroups from commands:

book_groups = data_source.command(commands.GetMessageGroups(book_id))

# For aliases, GetMessageAliases

aliases = data_source.command(commands.GetMessageAliases(book_id))

# Date has to be in utc timezone.
START_TIME = datetime(year=2022, month=11, day=10, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=10, hour=13, minute=53, second=8, microsecond=0)

# We use HTTPDataSource's command method to fetch different types of data from our provider.
# Most commands return data which we can store in th2 Data container.

# To fetch events/messages with ids we have 4 commands:

singleEvent = data_source.command(
    commands.GetEventById(
        "demo_book_1:th2-scope:20221226140719671764000:9c59694b-8526-11ed-8311-df33e1b504e4"
    )
)
multipleEvents = data_source.command(
    commands.GetEventsById(
        [
            "demo_book_1:th2-scope:20221226140719671764000:9c59694b-8526-11ed-8311-df33e1b504e4",
            "demo_book_1:th2-scope:20221226140723967243000:9ee8edcc-8526-11ed-8311-df33e1b504e4",
            "demo_book_1:th2-scope:20221226140724065522000:9ef7e1ed-8526-11ed-8311-df33e1b504e4",
        ]
    )
)

# # # Because namespace was taken down temporary lw-provider can't return messages.
# single_message = data_source.command(
#     commands.GetMessageById("case3:arfq02fix30:2:20221111165012889502000:1668182272676097251")
# )
# multiple_messages = data_source.command(
#     commands.GetMessagesById(
#         [
#             "case3:arfq02fix30:2:20221111165012889502000:1668182272676097251",
#             "case3:arfq02fix30:2:20221111165252889876000:1668182272676097315",
#         ]
#     )
# )

# We can get events without knowing their ids beforehand, using SSE requests from the server with GetEvents command:

events: Data = data_source.command(
    commands.GetEventsByBookByScopes(
        start_timestamp=START_TIME, end_timestamp=END_TIME, book_id=book_id, scopes=scopes
    )
)

START_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=53, second=8, microsecond=0)

# Similarly, to get messages we have two commands.
# First is GetMessagesByStream which returns messages witch matching aliases:

# # # Because namespace was taken down temporary lw-provider can't return messages.
# messages_by_stream: Data = data_source.command(
#     commands.GetMessagesByBookByStreams(
#         start_timestamp=START_TIME,
#         streams=Streams(streams),
#         end_timestamp=END_TIME,
#         book_id=book_id,
#     )
# )

# Other way is getting them by matching groups via GetMessagesByGroup:

# # # Because namespace was taken down temporary lw-provider can't return messages.
# messages_by_group: Data = data_source.command(
#     commands.GetMessagesByBookByGroups(
#         start_timestamp=START_TIME, groups=st, end_timestamp=END_TIME, book_id="case3"
#     )
# )

pages: Data = data_source.command(commands.GetPages(book_id, START_TIME, END_TIME))

page = list(pages)[0]

# # # Because namespace was taken down temporary lw-provider can't return messages.
# messages_by_page_by_streams = data_source.command(
#     commands.GetMessagesByPageByStreams(page, streams)
# )

# # # Because namespace was taken down temporary lw-provider can't return messages.
# messages_by_page_by_groups = data_source.command(
#     commands.GetMessagesByPageByGroups(page, groups)
# )

events_by_page_by_scopes = data_source.command(
    commands.GetEventsByPageByScopes(page, scopes=["th2-scope"])
)
