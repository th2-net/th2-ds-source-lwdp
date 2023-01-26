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
     - GetPages(book_id) can be used get all pages, specifying timestamps will only retrieve pages in given time frame.
   - GetByPage commands can now take page name (with book_id) to use it in command.

## Improvements

1. Updated required arguments for some commands and methods.
2. All the arguments that are List types are now in plural forms, e.g., stream -> streams,
   message_id -> message_ids.
3. Renamed GetMessages to GetMessagesByBookByStreams.
4. [TH2-4581] Added a check that commands start & end time parameters are milliseconds.
5. HTTP Commands changed:
   - GetEvents -> GetEventsByBookByScopes
   - GetMessages -> GetMessagesByBookByStreams / GetMessagesByBookByGroups
6. [TH2-4672] Removed ability to add custom sse handler in commands. Buffer limit can be changed.
7. [TH2-4651] HttpETCDriver added.
