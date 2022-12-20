from datetime import datetime
from th2_data_services_lwdp.data_source import HTTPDataSource
from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.commands import http as commands

ds_api = HTTPAPI("http://th2-kuber-test03:32681")
ds = HTTPDataSource("http://10.100.66.105:32681")

START_TIME = datetime(year=2022, month=11, day=10, hour=10, minute=50, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=11, day=11, hour=10, minute=55, second=8, microsecond=0)

book_id = "case3"
scope = ["th2-scope"]
stream = ["arfq02dc30"]

put = lambda func: print(f"\n{'-' * 10} {func} {'-' * 10}")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetEventScopes")
scopes = ds.command(commands.GetEventScopes(book_id))
print(scopes)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetMessageAliases")
aliases = ds.command(commands.GetMessageAliases(book_id))
print(aliases)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetMessageGroups")
msg_groups = ds.command(commands.GetMessageGroups(book_id))
print(msg_groups)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetPages")
pages = ds.command(commands.GetPages(book_id, datetime.fromtimestamp(1668013240), datetime.now()))
print(pages)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetMessagesByPageByStreams")
page = list(pages)[0]
try:
    messages = ds.command(commands.GetMessagesByPageByStreams(page, stream))
    print(messages)
except Exception as err:
    print(err)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetBooks")
books = ds.command(commands.GetBooks())
print(books)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetEventById")
event = ds.command(
    commands.GetEventById(
        "case3:th2-scope:20221110122124000000000:f41a8510-3904-4eeb-baa5-9870eae242ea"
    )
)
print(event)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetEventsById")
events = ds.command(
    commands.GetEventsById(
        [
            "case3:th2-scope:20221110122124000000000:f41a8510-3904-4eeb-baa5-9870eae242ea",
            "case3:th2-scope:20221110122224000000000:def0e516-cc13-4fa6-8be6-2b0873297c9c",
            "case3:th2-scope:20221110122324000000000:c3c5f7d5-e06d-4e23-98c9-7f36220d26db",
        ]
    )
)
print(events)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetEventsByBookByScopes")
events = ds.command(
    commands.GetEventsByBookByScopes(
        book_id=book_id, scopes=scope, start_timestamp=START_TIME, end_timestamp=END_TIME
    )
)
print(events)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetEventsByPageByScopes")
events = ds.command(
    commands.GetEventsByPageByScopes(
        page,
        scopes=scope,
    )
)
print(events)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetMessageById")
try:
    message = ds.command(
        commands.GetMessageById("case3:arfq02dc30:2:20221111165020871966000:1668182270286678628")
    )
    print(message)
except Exception as err:
    print(err)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetMessagesByBookByGroups")
try:
    messages = ds.command(
        commands.GetMessagesByBookByGroups(
            book_id=book_id,
            groups=["group1", "group2"],
            start_timestamp=START_TIME,
            end_timestamp=END_TIME,
        )
    )
    print(messages)
except Exception as err:
    print(err)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetEventsByPageByScopes")
events = ds.command(commands.GetEventsByPageByScopes(page, ["th2-scope"]))
print(events)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetMessagesByBookByStreams")
messages = ds.command(
    commands.GetMessagesByBookByStreams(
        start_timestamp=START_TIME, end_timestamp=END_TIME, streams=stream, book_id="case3"
    )
)
print(messages)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

messages = ds.command(commands.GetMessagesByPageByGroups(page, ["group1", "group2"]))
print(messages)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

put("GetMessagesByBookByStreams")
messages = ds.command(
    commands.GetMessagesByBookByStreams(
        book_id=book_id, streams=stream, start_timestamp=START_TIME, end_timestamp=END_TIME
    )
)
print(messages)
