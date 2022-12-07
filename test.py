from datetime import datetime, timezone
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.commands import http as commands
from th2_data_services_lwdp.source_api.http import HTTPAPI


ds = HTTPDataSource("http://th2-kuber-test03:32681")
dsa = HTTPAPI("http://th2-kuber-test03:32681")

START_TIME = datetime(year=2022, month=11, day=11, hour=12, minute=0, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=53, second=8, microsecond=0)
from urllib.parse import quote
testUrl = 'http://th2-kuber-test03:32681/search/sse/messages?startTimestamp=2022-11-11T12:00:00.000000Z&stream=arfq02fix30&searchDirection=next&endTimestamp=2022-11-11T16:53:08.000000Z&keepOpen=False&bookId=case3'
print("URL:",testUrl)
urlEncoded = testUrl.encode()
print("ENCODED:",urlEncoded)
urlAfterQueue = quote(urlEncoded,"/:&?=")
print("AFTERQUEUE",urlAfterQueue)

msgs = ds.command(commands.GetMessages(
    start_timestamp=START_TIME,
    end_timestamp=END_TIME,
    book_id='case3',
    stream=['arfq02fix30']))
for i in msgs:
    print(i)

