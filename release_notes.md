# v2.0.1.0

## Features
1. Updated all components so they are compatible with LwDP v2.
2. Added methods in source_api for generating all requests that exist on provider.
3. Added new commands: [TH2-4529] GetMessagesByGroups, GetBooks, GetMessageAliases, GetMessageGroups, GetEventScopes.
4. Updated old commands.
5. Streams class refactored.
6. Added new Stream class.

## Improvements
1. Updated required arguments for some commands and methods.
2. All the arguments that are List types are now in plural forms, e.g., stream -> streams, message_id -> message_ids.
3. Renamed GetMessages to GetMessagesByStreams.