# Necessary imports for demo.
from th2_data_services_lwdp.commands import grpc as commands
from th2_data_services_lwdp.data_source import GRPCDataSource
from th2_data_services import Data
from th2_data_services_lwdp.streams import Streams
from datetime import datetime

# Create grpc data source object to connect to lightweight data provider.
DEMO_HOST = "de-th2-qa"  # Note that this host is only accessible from exactpro domain
DEMO_PORT = "31038"
data_source = GRPCDataSource(f"{DEMO_HOST}:{DEMO_PORT}")

# Date has to be in utc timezone.
START_TIME = datetime(year=2022, month=7, day=1, hour=14, minute=0, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=8, day=1, hour=14, minute=0, second=0, microsecond=0)

# We use GRPCDataSource's command method to fetch different types of data from our provider.
# Most commands return data which we can store in th2 Data container.

# We can get events from the server with GetEvents command:
# -- start_timestamp (required) - start date of search range.
# -- end_timestamp (required) - end date of search range.
# -- parent_event - matches only events with specific parent event.
# -- cache - if true, the requested data will be saved locally (default=False).

events: Data = data_source.command(
    commands.GetEvents(start_timestamp=START_TIME, end_timestamp=END_TIME)
)

# Similarly we can fetch messages via GetMessages command:
# -- start_timestamp (required) - start date of search range.
# -- stream (required) - this argument must be a list of either strings or Streams object.
#       We can use get_message_streams method from provider_api to find names of streams.
# -- search_direction - next or previous - sets the direction of lookup - default is next.
# -- result_count_limit - Sets the maximum number of returned messages - by default it is unlimited.
# -- end_timestamp - end date of search range.
# -- stream_pointers - List of stream pointers to restore the search from.
#       start_timestamp will be ignored if this parameter is specified.
# -- cache - if true, the requested data will be saved locally (default=False).

example_stream = [Streams(["newretest9"])]

messages: Data = data_source.command(
    commands.GetMessages(start_timestamp=START_TIME, stream=example_stream, end_timestamp=END_TIME)
)
