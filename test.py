from th2_data_services_lwdp.events_tree.events_tree_collection import EventsTreeCollectionProvider as ETC
from th2_data_services_lwdp.commands import grpc as commands
from th2_data_services_lwdp.data_source import GRPCDataSource
from th2_data_services import Data
from datetime import datetime

START_TIME = datetime(year=2022, month=6, day=30, hour=14, minute=0, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=6, day=30, hour=15, minute=0, second=0, microsecond=0)

data_source = GRPCDataSource("de-th2-qa:31038")
events: Data = data_source.command(commands.GetEvents(start_timestamp=START_TIME,end_timestamp=END_TIME))
print(events)