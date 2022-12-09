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

# Date has to be in utc timezone.
START_TIME = datetime(year=2022, month=11, day=10, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=10, hour=13, minute=53, second=8, microsecond=0)

# We use HTTPDataSource's command method to fetch different types of data from our provider.
# Most commands return data which we can store in th2 Data container.

# We can get events from the server with GetEvents command:

events: Data = data_source.command(
    commands.GetEvents(start_timestamp=START_TIME, end_timestamp=END_TIME, book_id="case3", scopes="th2-scope")
)

print(events.len)

START_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=53, second=8, microsecond=0)

# To get messages we have two commands.
# First is GetMessagesByStream which returns messages witch matching aliases:
example_stream = [Streams(["arfq02fix30"])]

messagesByStream: Data = data_source.command(
    commands.GetMessagesByStreams(start_timestamp=START_TIME, streams=example_stream, end_timestamp=END_TIME, book_id="case3")
)

print(messagesByStream.len)

#Other way is getting them by matching groups via GetMessagesByGroup:

messagesByGroup: Data = data_source.command(
    commands.GetMessagesByGroups(start_timestamp=START_TIME, groups=["Case032"], end_timestamp=END_TIME, book_id="case3")
)

print(messagesByGroup.len)