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

# v2.1.0.0

## User impact and migration instructions

1. [I] GetMessageAliases and GetMessageGroups commands now return a TH2 Data object instead of a list.
   It means that using list functions on these commands' return values won't work anymore and must be
   treated as Data objects.
   [M] Update your usage of these commands' returned objects and don't use list methods on them, instead
   use Data object's functions or cast them into a list first.

## Features

1. [TH2-4924] Added GetMessagesByPage command.

## Improvements

1. [TH2-4922] GetMessageAliases command now takes optional start_timestamp and end_timestamp arguements and returns TH2 Data object instead of a list.
2. [TH2-4923] GetMessageGroups command now takes optional start_timestamp and end_timestamp arguements and returns TH2 Data object instead of a list.
3. [TH2-4924] Added GetMessagesByPages command.
4. [TH2-4926] Added GetEventsByPages command.

## BugFixes
1. [TH2-4925] Fix BrokenEvent and BrokenMessage.

