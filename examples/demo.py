# Necessary imports for demo.
from th2_data_services_lwdp.commands import http as commands
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services import Data
from th2_data_services_lwdp.streams import Streams
from datetime import datetime

# Create grpc data source object to connect to lightweight data provider.
DEMO_HOST = "10.100.66.105"  # Note that this host is only accessible from exactpro domain
DEMO_PORT = "32681"
data_source = HTTPDataSource(f"http://{DEMO_HOST}:{DEMO_PORT}")

# On database data is segregated with books, such as they never interesct.
# To get the names of the books we have a command GetBooks which takes no argument:

books = data_source.command(commands.GetBooks())

# In books data is parititioned even more.
# Events are grouped by scopes, which we can get using GetScopes command:

scopes = data_source.command(
    commands.GetEventScopes("case3")
)  # case3 is an example book from host namespace

# Messages are separated by groups and aliases.
# To get groups we use GetMessageGroups from commands:

groups = data_source.command(commands.GetMessageGroups("case3"))

# For aliases, GetMessageAliases

aliases = data_source.command(commands.GetMessageAliases("case3"))

# Date has to be in utc timezone.
START_TIME = datetime(year=2022, month=11, day=10, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=10, hour=13, minute=53, second=8, microsecond=0)

# We use HTTPDataSource's command method to fetch different types of data from our provider.
# Most commands return data which we can store in th2 Data container.

# To fetch events/messages with ids we have 4 commands:

singleEvent = data_source.command(
    commands.GetEventById(
        "case3:th2-scope:20221110122224000000000:def0e516-cc13-4fa6-8be6-2b0873297c9c"
    )
)
multipleEvents = data_source.command(
    commands.GetEventsById(
        [
            "case3:th2-scope:20221110122224000000000:def0e516-cc13-4fa6-8be6-2b0873297c9c",
            "case3:th2-scope:20221110122324000000000:c3c5f7d5-e06d-4e23-98c9-7f36220d26db",
        ]
    )
)

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

# We can get events without knowing their ids beforehands, using SSE requests from the server with GetEvents command:

events: Data = data_source.command(
    commands.GetEventsByBookByScopes(
        start_timestamp=START_TIME, end_timestamp=END_TIME, book_id="case3", scopes=["th2-scope"]
    )
)

START_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=53, second=8, microsecond=0)

# Similarly, to get messages we have two commands.
# First is GetMessagesByStream which returns messages witch matching aliases:
example_stream = []

messages_by_stream: Data = data_source.command(
    commands.GetMessagesByBookByStreams(
        start_timestamp=START_TIME,
        streams=Streams(["arfq02fix30"]),
        end_timestamp=END_TIME,
        book_id="case3",
    )
)

# Other way is getting them by matching groups via GetMessagesByGroup:

messages_by_group: Data = data_source.command(
    commands.GetMessagesByBookByGroups(
        start_timestamp=START_TIME, groups=["arfq02dc30"], end_timestamp=END_TIME, book_id="case3"
    )
)
