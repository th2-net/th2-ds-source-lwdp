from datetime import datetime, timezone
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.source_api.http import HTTPAPI

ds = HTTPDataSource("th2-kuber-test03:30452")

START_TIME = datetime(year=2022, month=11, day=16, hour=12, minute=53, second=1, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=16, hour=12, minute=53, second=8, microsecond=0)
st =  int(START_TIME.replace(tzinfo=timezone.utc).timestamp() * 10**9)
et =  int(END_TIME.replace(tzinfo=timezone.utc).timestamp() * 10**9)
events = ds.source_api.search_events(start_timestamp=st,end_timestamp=et)
for i in events:
    print(i)