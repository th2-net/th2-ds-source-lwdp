
from datetime import datetime, timezone, timedelta
from th2_data_services_lwdp.data_source import HTTPDataSource

from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.filters.filter import LwDPFilter
all_message_bodies_http = [{
	'timestamp': {
		'epochSecond': 1672927025,
		'nano': 556859000
	},
	'direction': 'OUT',
	'sessionId': 'ds-lib-session1',
	'messageType': 'Outgoing',
	'attachedEventIds': [],
	'body': {
		'metadata': {
			'id': {
				'connectionId': {
					'sessionAlias': 'ds-lib-session1'
				},
				'direction': 'SECOND',
				'sequence': '1672927025546507568',
				'timestamp': {
					'seconds': '1672927025',
					'nanos': '556859000'
				},
				'subsequence': []
			},
			'messageType': 'Outgoing',
			'protocol': 'json'
		},
		'fields': {
			'a': {
				'simpleValue': '123'
			}
		}
	},
	'bodyBase64': 'eyJhIjogIjEyMyJ9',
	'messageId': 'demo_book_1:ds-lib-session1:2:20230105135705556859000:1672927025546507568'
}, {
	'timestamp': {
		'epochSecond': 1672927025,
		'nano': 546983000
	},
	'direction': 'IN',
	'sessionId': 'ds-lib-session1',
	'messageType': 'Incoming',
	'attachedEventIds': [],
	'body': {
		'metadata': {
			'id': {
				'connectionId': {
					'sessionAlias': 'ds-lib-session1'
				},
				'direction': 'FIRST',
				'sequence': '1672927025546507566',
				'timestamp': {
					'seconds': '1672927025',
					'nanos': '546983000'
				},
				'subsequence': []
			},
			'messageType': 'Incoming',
			'protocol': 'json'
		},
		'fields': {
			'a': {
				'simpleValue': '123'
			}
		}
	},
	'bodyBase64': 'eyJhIjogIjEyMyJ9',
	'messageId': 'demo_book_1:ds-lib-session1:1:20230105135705546983000:1672927025546507566'
}, {
	'timestamp': {
		'epochSecond': 1672927025,
		'nano': 559347000
	},
	'direction': 'IN',
	'sessionId': 'ds-lib-session1',
	'messageType': 'Incoming',
	'attachedEventIds': [],
	'body': {
		'metadata': {
			'id': {
				'connectionId': {
					'sessionAlias': 'ds-lib-session1'
				},
				'direction': 'FIRST',
				'sequence': '1672927025546507570',
				'timestamp': {
					'seconds': '1672927025',
					'nanos': '559347000'
				},
				'subsequence': []
			},
			'messageType': 'Incoming',
			'protocol': 'json'
		},
		'fields': {
			'msg_for_get_by_id_num': {
				'simpleValue': '1'
			}
		}
	},
	'bodyBase64': 'eyJtc2dfZm9yX2dldF9ieV9pZF9udW0iOiAiMSJ9',
	'messageId': 'demo_book_1:ds-lib-session1:1:20230105135705559347000:1672927025546507570'
}, {
	'timestamp': {
		'epochSecond': 1672927025,
		'nano': 559529000
	},
	'direction': 'IN',
	'sessionId': 'ds-lib-session1',
	'messageType': 'Incoming',
	'attachedEventIds': [],
	'body': {
		'metadata': {
			'id': {
				'connectionId': {
					'sessionAlias': 'ds-lib-session1'
				},
				'direction': 'FIRST',
				'sequence': '1672927025546507571',
				'timestamp': {
					'seconds': '1672927025',
					'nanos': '559529000'
				},
				'subsequence': []
			},
			'messageType': 'Incoming',
			'protocol': 'json'
		},
		'fields': {
			'msg_for_get_by_id_num': {
				'simpleValue': '2'
			}
		}
	},
	'bodyBase64': 'eyJtc2dfZm9yX2dldF9ieV9pZF9udW0iOiAiMiJ9',
	'messageId': 'demo_book_1:ds-lib-session1:1:20230105135705559529000:1672927025546507571'
}, {
	'timestamp': {
		'epochSecond': 1672927025,
		'nano': 557841000
	},
	'direction': 'OUT',
	'sessionId': 'ds-lib-session2',
	'messageType': 'Outgoing',
	'attachedEventIds': [],
	'body': {
		'metadata': {
			'id': {
				'connectionId': {
					'sessionAlias': 'ds-lib-session2'
				},
				'direction': 'SECOND',
				'sequence': '1672927025546507569',
				'timestamp': {
					'seconds': '1672927025',
					'nanos': '557841000'
				},
				'subsequence': []
			},
			'messageType': 'Outgoing',
			'protocol': 'json'
		},
		'fields': {
			'a': {
				'simpleValue': '123'
			}
		}
	},
	'bodyBase64': 'eyJhIjogIjEyMyJ9',
	'messageId': 'demo_book_1:ds-lib-session2:2:20230105135705557841000:1672927025546507569'
}, {
	'timestamp': {
		'epochSecond': 1672927025,
		'nano': 555306000
	},
	'direction': 'IN',
	'sessionId': 'ds-lib-session2',
	'messageType': 'Incoming',
	'attachedEventIds': [],
	'body': {
		'metadata': {
			'id': {
				'connectionId': {
					'sessionAlias': 'ds-lib-session2'
				},
				'direction': 'FIRST',
				'sequence': '1672927025546507567',
				'timestamp': {
					'seconds': '1672927025',
					'nanos': '555306000'
				},
				'subsequence': []
			},
			'messageType': 'Incoming',
			'protocol': 'json'
		},
		'fields': {
			'a': {
				'simpleValue': '123'
			}
		}
	},
	'bodyBase64': 'eyJhIjogIjEyMyJ9',
	'messageId': 'demo_book_1:ds-lib-session2:1:20230105135705555306000:1672927025546507567'
}]
ds_api = HTTPAPI("http://th2-kuber-test03:32681")

START_TIME = datetime(year=2022, month=11, day=10, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=16, minute=53, second=8, microsecond=0)
st = int(START_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)
et = int(END_TIME.replace(tzinfo=timezone.utc).timestamp() * 1000)

# url = ds.command(
#     commands.GetMessagesByBookByStreams(
#         start_timestamp=st, end_timestamp=et, streams=streams, book_id="case3"
#     )
# )
# for i in url:
#     print(i)

# # Will Return `408 Status Code` And MessageNotFound Will Be Raised
#message = ds.command(
#    commands.GetEventById("demo_book_1:th2-scope:20230105135705560873000:d61e930a-8d00-11ed-aa1a-d34a6155152d_1")
#)
#print(message)
#pages = ds.command(commands.GetPages("demo_book_1", start_timestamp=START_TIME, end_timestamp=END_TIME))

#messages = ds.command(
 #   commands.GetMessagesByPageByGroups(page=list(pages)[0],groups=['ds-lib-session1','ds-lib-session2'])
#)

#print(len(list(messages)))
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.commands import http as commands

ds = HTTPDataSource("http://th2-kuber-test03:32681")

START_TIME = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=5, microsecond=0) 
END_TIME   = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=6, microsecond=0)  

BOOK_NAME='demo_book_1'
STREAMS = ['ds-lib-session1','ds-lib-session2']
SCOPES = ['th2-scope']
comm = commands.GetMessagesByBookByStreams(
                start_timestamp=START_TIME,
                end_timestamp=END_TIME,
                streams=STREAMS,
                book_id=BOOK_NAME,
            )

data = ds.command(comm)
for i in data:
	print(i['messageId'])

#page = list(pages)[0]
# s = time.time()
# # Will Return `error` Event
# messages = ds.command(
#     commands.GetMessagesByPageByStreams(
#         page, ["arfq02dc30"]
#     )
# )
# print(messages)
# print(time.time() - s)

# page = list(pages)[0] 
# s = time.time()
# messages = ds.command(
#     commands.GetMessagesByPageByGroups(
#         page, ["group1", "group2"]
#     )
# )
# print(messages)
# print(time.time() - s)


# messages = ds.command(commands.GetEventsByPageByScopes(page, ["th2-scope"]))
# print(messages)