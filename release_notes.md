# v2.0.1.0

## Features

1. Updated all components so that they are compatible with LwDP v2.
2. Added methods in source_api for generating all requests that exist on provider.
3. [TH2-4529] Added new commands:
    - GetMessagesByGroups
    - GetBooks
    - GetMessageAliases
    - GetMessageGroups
    - GetEventScopes
4. Added new `Stream` and `Streams` class.
5. [TH2-4535] Added new http commands:
    - GetPages
    - GetEventsByPageByScopes
    - GetMessagesByPageByStreams
    - GetMessagesByPageByGroups
6. [TH2-4615] Data class now contains api request urls in metadata.
7. [TH2-4625]
    - Commands added:
        - GetPageByName command added to get Page by name with book_id
        - GetPages(book_id) can be used get all pages, specifying timestamps will only retrieve
          pages in given time frame.
    - GetByPage commands can now take page name (with book_id) to use it in command.
8. [TH2-4651] HttpETCDriver added.

## Improvements

1. Updated required arguments for some commands and methods.
2. All the arguments that are List types are now in plural forms, e.g., stream -> streams,
   message_id -> message_ids.
3. Renamed GetMessages to GetMessagesByBookByStreams.
4. HTTP Commands changed:
    - GetEvents -> GetEventsByBookByScopes
    - GetMessages -> GetMessagesByBookByStreams / GetMessagesByBookByGroups
5. [TH2-4672] Removed ability to add custom sse handler in commands. Buffer limit can be changed.
6. [TH2-4753] Commands now send timestamps with nanoseconds precision instead of milliseconds.
7. [TH2-4754] Copyright checker added to pre-commit

# v2.0.1.1

## Improvements

1. [TH2-4806] Added vulnerabilities scanning

# v2.0.2.0

## User impact and migration instructions

1. [I] Stub builders create stub events and messages with BrokenEvent/BrokenMessages objects instead
   of strings. It means that the old check like `if stub_event["eventName"] == "Broken_Event"` will
   not work more. Use `if stub_event["eventName"] == BrokenEvent`.
   [M] Update your string `Broken_Event` check to class BrokenEvent. The same for messages.

## Features

1. [TH2-4869] Added ResponseFormat class to provide possible values for response_formats commands
   parameter.
2. [TH2-4692] Added response_formats for GetMessageById and GetMessagesById and fixed for SSE
   commands.
3. [TH2-4882] Added get_download_messages API.

## Improvements

1. [TH2-4869] ResponseFormat == 'JSON_PARSED' made as default.
2. [TH2-4790] Stub messages and events don't use "Broken_event" and "Broken_message" strings
   anymore.
   Use BrokenEvent and BrokenMessage objects instead.

# v2.0.3.0

## Features

1. [TH2-5027] `expand_message` added to LwdpMessageFieldsResolver.

# v2.0.3.1

BugFixes without ticket


# v2.1.0.0

## User impact and migration instructions

1. [I] GetMessageAliases and GetMessageGroups commands now return a TH2 Data object instead of a
   list.
   It means that using list functions on these commands' return values won't work anymore and must
   be
   treated as Data objects.
   [M] Update your usage of these commands' returned objects and don't use list methods on them,
   instead
   use Data object's functions or cast them into a list first.

## Features

1. [TH2-4924] Added GetMessagesByPage command.
2. [TH2-4924] Added GetMessagesByPages command.
3. [TH2-4926] Added GetEventsByPages command.
4. [TH2-4952] Added DownloadMessagesByPageByGroups, DownloadMessagesByPage and
   DownloadMessagesByBookByGroups commands.
5. [Th2-4975] Added streams parameter to download and get-messages-by-groups/pages sse commands.
   That allows to

## Improvements

1. [TH2-4922] GetMessageAliases command now takes optional start_timestamp and end_timestamp
   arguements and returns TH2 Data object instead of a list.
2. [TH2-4923] GetMessageGroups command now takes optional start_timestamp and end_timestamp
   arguements and returns TH2 Data object instead of a list.
3. [TH2-4924] Added GetMessagesByPages command.
4. [TH2-4926] Added GetEventsByPages command.
5. [TH2-4952] Added DownloadMessagesByPageByGroups, DownloadMessagesByPage and
   DownloadMessagesByBookByGroups commands.
6. [Th2-4975] Added streams parameter to download and messages by groups sse commands.
7. [TH2-5027] `expand_message` added to MessageFieldResolver.

## BugFixes

1. [TH2-4925] Fix BrokenEvent and BrokenMessage.

# v2.1.0.1

## BugFixes

1. Fix DownloadMessages commands

# v2.1.0.2

## BugFixes

1. Use options.setup_resolvers function instead of strict variable assignment

# v2.1.0.3

## BugFixes

1. Raise exception if LwDP returns Error HTTP status in Download commands.
2. Added check for gzip commands filename arguments, checks if filename already has .gz at the end
   doesn't add second extension if so.

## Features

1. [TH2-5049] Added ExpandedMessageFieldResolver

## Improvements

1. [TH2-5048] - Added typing hints for resolver methods


# v2.1.0.4

## BugFixes

1. [TH2-5077] - Fix wrong check that breaks GetMessagesByBookByGroups & GetMessagesByPageByGroups commands.


# v3.0.1.0

## User impact and migration instructions

1. [I] GRPC commands removed entirely.
   [M] Usage of these commands should be removed or replaced by http counterparts.

2. [I] In LwDP v3 message structure is changed.
   [M] Accessing message fields should be changed if they are accessed without using resolver. It's also better to use message field expander, instead of directly accessing 'body' field. 

## Features

1. [Th2-4975] Added streams parameter to download and get-messages-by-groups/pages sse commands.
2. [TH2-5049] Added ExpandedMessageFieldResolver.

## Improvements

1. [TH2-4969] - Updated MessageStubBuilder & LwdpMessageFieldsResolver.
2. [TH2-4945] - Refactoring: Remove GRPC.
3. [TH2-4959] - Added deprecation warning.
4. [TH2-5048] - Added typing hints for resolver methods.
5. [TH2-4974] - Added resolver for getting group in message.


# v3.0.2.0

## Improvements

1. [TH2-5140] `Download messages` commands now use new endpoints to download messages. 
   Tasks are created for each download request. If something goes wrong and task fails its 
   status can now be seen in Data object returned by download commands.
2. `Download messages` commands store the messages on the disc and return `Data` object now. 
   So you don't need to read the file manually. 
3. Changed the way how to init the module resolvers. 
   We made it like it works in the `traceback_with_variables` library. 
   Now you can import them just importing `init_resolvers_by_import` module.
   Example: `from th2_data_services.data_source.lwdp import init_resolvers_by_import`
   Old approach via importing `from th2_data_services.data_source.lwdp` also is working.
4. Highly improved the downloading speed of GetEventsById & GetMessagesById. 
   They are work via async queries now.
   Speed tests for GetEventsById:
      - old sync approach: 75sec to download 100 Events by Ids
      - new async approach: 1.5sec to download 100 Events by Ids, 13sec to download 1000 Events
   