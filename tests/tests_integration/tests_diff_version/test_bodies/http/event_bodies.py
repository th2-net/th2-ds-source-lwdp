root_event_body = {
	'eventId': 'demo_book_1:th2-scope:20230105135705560873000:d61e930a-8d00-11ed-aa1a-d34a6155152d_1',
	'batchId': None,
	'isBatched': False,
	'eventName': 'Set of auto-generated events for ds lib testing',
	'eventType': 'ds-lib-test-event',
	'endTimestamp': {
		'epochSecond': 1672927025,
		'nano': 561751000
	},
	'startTimestamp': {
		'epochSecond': 1672927025,
		'nano': 560873000
	},
	'parentEventId': None,
	'successful': True,
	'bookId': 'demo_book_1',
	'scope': 'th2-scope',
	'attachedMessageIds': [],
	'body': []
}

plain_event_1_body = {
	'eventId': 'demo_book_1:th2-scope:20230105135705563522000:9adbb3e0-5f8b-4c28-a2ac-7361e8fa704c>demo_book_1:th2-scope:20230105135705563522000:d61e930a-8d00-11ed-aa1a-d34a6155152d_2',
	'batchId': 'demo_book_1:th2-scope:20230105135705563522000:9adbb3e0-5f8b-4c28-a2ac-7361e8fa704c',
	'isBatched': True,
	'eventName': 'Plain event 1',
	'eventType': 'ds-lib-test-event',
	'endTimestamp': {
		'epochSecond': 1672927025,
		'nano': 563640000
	},
	'startTimestamp': {
		'epochSecond': 1672927025,
		'nano': 563522000
	},
	'parentEventId': 'demo_book_1:th2-scope:20230105135705560873000:d61e930a-8d00-11ed-aa1a-d34a6155152d_1',
	'successful': True,
	'bookId': 'demo_book_1',
	'scope': 'th2-scope',
	'attachedMessageIds': [],
	'body': {
		'type': 'message',
		'data': 'ds-lib test body'
	}
}

filter_event_3_body = {
	'eventId': 'demo_book_1:th2-scope:20230105135705566148000:6d33884e-709c-48fe-81b8-72f8d0ce7d85>demo_book_1:th2-scope:20230105135705566350000:d61e930a-8d00-11ed-aa1a-d34a6155152d_14',
	'batchId': 'demo_book_1:th2-scope:20230105135705566148000:6d33884e-709c-48fe-81b8-72f8d0ce7d85',
	'isBatched': True,
	'eventName': 'Event for Filter test. FilterString-3',
	'eventType': 'ds-lib-test-event',
	'endTimestamp': {
		'epochSecond': 1672927025,
		'nano': 566373000
	},
	'startTimestamp': {
		'epochSecond': 1672927025,
		'nano': 566350000
	},
	'parentEventId': 'demo_book_1:th2-scope:20230105135705560873000:d61e930a-8d00-11ed-aa1a-d34a6155152d_1',
	'successful': True,
	'bookId': 'demo_book_1',
	'scope': 'th2-scope',
	'attachedMessageIds': [],
	'body': '"ds-lib test body. FilterString-3"'
}