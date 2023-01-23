from th2_data_services_lwdp.commands import http as commands
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services import Data
from datetime import datetime

from th2_data_services_lwdp.streams import Streams, Stream

# [1] Create data source object to connect to lightweight data provider.
DEMO_HOST = "10.100.66.105"  # Note that this host is only accessible from exactpro domain
DEMO_PORT = "32681"
data_source = HTTPDataSource(f"http://{DEMO_HOST}:{DEMO_PORT}")

# [2] Getting books, pages, scopes, groups and aliases.
# [2.1] Get books.
# On database data is segregated with books, such as they never intersect.
# To get the names of the books we have a command GetBooks which takes no argument:
books = data_source.command(commands.GetBooks())

# [2.2] Initialize name variables.
book_id = "demo_book_1"  # demo_book_1 is an example book from host namespace
page_name = "1"  # example page name from book demo_book_1
scopes = ["th2-scope"]
# For SSE GetMessageByStream commands we have three choices for streams argument:
# We can use a list of strings.
streams = [
    "default-message-producer-alias",
    "fix-demo-server1",
    "fix-demo-server2",
    "fix-client2",
    "fix-client1",
]
# A list of Stream objects.
streams_list_with_stream_object = [
    Stream("default-message-producer-alias"),
    Stream("fix-demo-server1"),
    Stream("fix-demo-server2"),
    Stream("fix-client1"),
    Stream("fix-client2"),
]
# Or a Streams object, which takes a list of strings as argument to initialize.
streams_with_streams_class = Streams(streams)

groups = streams  # In this namespace groups and streams have same name.

# Date has to be in utc timezone.
START_TIME = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=5, microsecond=0)
END_TIME = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=6, microsecond=0)

# [2.3] Get pages.
# This command returns objects of Page class
# GetPages with only book_id will get all pages.
pages_all: Data = data_source.command(commands.GetPages(book_id))

# GetPages with timestamps will get all pages within that time frame.
pages: Data = data_source.command(commands.GetPages(book_id, START_TIME, END_TIME))

# [2.4] Get scopes.
# In books data is partitioned even more.
# Events are grouped by scopes, which we can get using GetScopes command:
book_scopes = data_source.command(commands.GetEventScopes(book_id))

# [2.5] Get message groups.
# Messages are separated by groups and aliases.
# To get groups we use GetMessageGroups from commands:
book_groups = data_source.command(commands.GetMessageGroups(book_id))

# [2.6] Get message aliases.
# For aliases, GetMessageAliases.
aliases = data_source.command(commands.GetMessageAliases(book_id))

# [3] Getting events and messages
# Most commands return data which we can store in th2 Data container.

# [3.1] Getting events by id.
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

# [3.2] Getting messages by id.
single_message = data_source.command(
    commands.GetMessageById("case3:arfq02fix30:2:20221111165012889502000:1668182272676097251")
)
multiple_messages = data_source.command(
    commands.GetMessagesById(
        [
            "case3:arfq02fix30:2:20221111165012889502000:1668182272676097251",
            "case3:arfq02fix30:2:20221111165252889876000:1668182272676097315",
        ]
    )
)

# [3.3] SSE commands for events.
# We can get events without knowing their ids beforehand, using SSE requests from the server with GetEvents command:
events: Data = data_source.command(
    commands.GetEventsByBookByScopes(
        start_timestamp=START_TIME, end_timestamp=END_TIME, book_id=book_id, scopes=scopes
    )
)

# [3.4] SSE commands for messages.
# Similar to events we have SSE commands for messages.
# GetMessagesByBookByStreams returns messages witch matching aliases:
messages_by_stream: Data = data_source.command(
    commands.GetMessagesByBookByStreams(
        start_timestamp=START_TIME,
        streams=Streams(streams),
        end_timestamp=END_TIME,
        book_id=book_id,
    )
)

# Other way is getting them by matching groups via GetMessagesByBookByGroups:
messages_by_group: Data = data_source.command(
    commands.GetMessagesByBookByGroups(
        start_timestamp=START_TIME, end_timestamp=END_TIME, groups=groups, book_id=book_id
    )
)

# [3.5] Getting data by pages.
# For every SSE command we have other variation, which matches data by specific page instead of datetime range.

page = list(pages)[0]

# GetByPage commands accept Page class objects as argument.
# Alternatively they also accept page name with book id.
events_by_page_by_scopes = data_source.command(
    commands.GetEventsByPageByScopes(page, scopes=["th2-scope"])
)

messages_by_page_by_streams = data_source.command(
    commands.GetMessagesByPageByStreams(page, streams)
)

messages_by_page_by_groups = data_source.command(commands.GetMessagesByPageByGroups(page, groups))


messages_by_page_name_by_streams = data_source.command(
    commands.GetMessagesByPageByStreams(page=page_name, book_id=book_id, stream=streams)
)

messages_by_page_name_by_groups = data_source.command(
    commands.GetMessagesByPageByGroups(page=page_name, book_id=book_id, groups=groups)
)

events_by_page_name_by_scopes = data_source.command(
    commands.GetEventsByPageByScopes(page=page_name, book_id=book_id, scopes=["th2-scope"])
)
