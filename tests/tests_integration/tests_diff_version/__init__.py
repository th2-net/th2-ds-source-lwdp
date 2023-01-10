from datetime import datetime
from importlib.metadata import version, PackageNotFoundError

v = version("th2_grpc_data_provider")

STREAM_1 = "ds-lib-session1"
STREAM_2 = "ds-lib-session2"

BOOK_NAME = "demo_book_1"
SCOPE = "th2-scope"

from th2_data_services_lwdp.data_source.http import HTTPDataSource
from th2_data_services_lwdp.commands import http  # noqa
from th2_data_services_lwdp.source_api import HTTPAPI  # noqa
from th2_data_services_lwdp.struct import http_message_struct as message_struct
from th2_data_services_lwdp.filters.filter import LwDPFilter as Filter

from .test_bodies.http.all_test_event_bodies import all_event_bodies_http
from .test_bodies.http.all_test_message_bodies import all_message_bodies_http


START_TIME = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=5, microsecond=0) 
END_TIME   = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=6, microsecond=0)  

EVENT_ID_TEST_DATA_ROOT = 'demo_book_1:th2-scope:20230105135705560873000:d61e930a-8d00-11ed-aa1a-d34a6155152d_1' 
EVENT_ID_PLAIN_EVENT_1 = 'demo_book_1:th2-scope:20230105135705563522000:9adbb3e0-5f8b-4c28-a2ac-7361e8fa704c>demo_book_1:th2-scope:20230105135705563522000:d61e930a-8d00-11ed-aa1a-d34a6155152d_2' 
EVENT_ID_PLAIN_EVENT_2 = 'demo_book_1:th2-scope:20230105135705563522000:9adbb3e0-5f8b-4c28-a2ac-7361e8fa704c>demo_book_1:th2-scope:20230105135705563757000:d61e930a-8d00-11ed-aa1a-d34a6155152d_3'  

MESSAGE_ID_1 = 'demo_book_1:ds-lib-session1:1:20230105135705559347000:1672927025546507570' 
MESSAGE_ID_2 = 'demo_book_1:ds-lib-session1:1:20230105135705559529000:1672927025546507571'

HTTP_PORT = "32681"  # HTTP lwdp
