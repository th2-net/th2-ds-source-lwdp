from datetime import datetime, timezone
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.commands import http as commands

# ds = HTTPDataSource("http://th2-kuber-test03:30452")
ds = HTTPDataSource("http://10.100.66.105:32681")

START_TIME = datetime(year=2022, month=11, day=10, hour=2, minute=53, second=1, microsecond=0)
END_TIME = datetime(year=2022, month=12, day=1, hour=22, minute=53, second=8, microsecond=0)
st = int(START_TIME.replace(tzinfo=timezone.utc).timestamp() * 10 ** 9)
et = int(END_TIME.replace(tzinfo=timezone.utc).timestamp() * 10 ** 9)

events = ds.command(commands.GetEvents(start_timestamp=START_TIME, end_timestamp=END_TIME))

for i in events:
    print(i)
