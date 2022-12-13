import time
from datetime import datetime,timezone

from th2_data_services import Data

from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.commands import http as commands

ds_api = HTTPAPI("http://th2-kuber-test03:32681")
ds = HTTPDataSource("http://10.100.66.105:32681")

START_TIME = datetime(year=2022, month=11, day=10, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=10, hour=13, minute=53, second=8, microsecond=0)
st = int(START_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)
et = int(END_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)

groups = ["core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
,"core_smoke","group_smoke","test12case","test13case","test2case","test3case","test5case","test6case","test8case","test9case","testpages15"
]

url = ds_api.get_url_search_messages_by_groups(start_timestamp=st,end_timestamp=et,groups=groups,book_id='case3')
for i in url:
    print(i)