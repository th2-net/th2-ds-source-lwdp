from th2_data_services_lwdp.commands import grpc as commands
from th2_data_services_lwdp.data_source import GRPCDataSource
from th2_data_services import Data
from th2_data_services_lwdp.streams import Streams
from datetime import datetime

START_TIME = datetime(year=2022, month=3, day=30, hour=14, minute=0, second=0, microsecond=0)
END_TIME = datetime.today()

data_source = GRPCDataSource("de-th2-qa:31038")
s = Streams(['newretest9'])
messages: Data = data_source.command(commands.GetMessages(start_timestamp=START_TIME,end_timestamp=END_TIME,stream=['newretest9']))

print(messages)