# 1. Introduction

This repository is a library for implementing LwDP data source.

The library uses ["Lightweight Data Provider"](https://github.com/th2-net/th2-lw-data-provider) as provider.

# 2. Getting started

## 2.1. Installation

- From PyPI (pip)   
  This package can be found on [PyPI](https://pypi.org/project/th2-data-services-lwdp/ "th2-data-services-lwdp").
    ```
    pip install th2-data-services-lwdp
    ```

- From Source
    ```
    git clone https://github.com/th2-net/th2-ds-source-lwdp
    pip install th2-data-services-lwdp
    ```

## 2.2. Releases

Currently there is only [ds-lwdp v1](https://github.com/th2-net/th2-ds-source-lwdp/tree/dev_1.0.1.0) under developement.

Newer releases will have separate branches indicated by SourceVersion of branch name.

# 2.3 Versioning

Versioning of the library looks like this:

DataSourceMajorVersion.LibVerison

DataSourceMajorVersion - the major version of LwDP the release uses
LibVersion - the version of data source implementation in Major.Minor.Patch versioning semantic style

## 2.4. Example

Here is a small example using a few of the module's structures.

[The following example as a file](https://github.com/th2-net/th2-ds-source-lwdp/tree/dev_1.0.1.0/examples/demo.py).

```python
# necessary imports for demo
from th2_data_services_lwdp.commands import grpc as commands
from th2_data_services_lwdp.data_source import GRPCDataSource
from th2_data_services import Data
from th2_data_services_lwdp.streams import Streams
from datetime import datetime

# create grpc data source object to connect to lightweight data provider
DEMO_HOST = 'de-th2-qa' # note that this host is only accessible from exactpro domain
DEMO_PORT = '31038' # node port for lwdp
data_source = GRPCDataSource(f"{DEMO_HOST}:{DEMO_PORT}")

# date has to be in utc timezone
START_TIME = datetime(year=2022, month=7, day=1, hour=14, minute=0, second=0, microsecond=0)
END_TIME = datetime(year=2022, month=8, day=1, hour=14, minute=0, second=0, microsecond=0)

# we use GRPCDataSource's command method to fetch different types of data from our provider.
# most commands return data which we can store in th2 Data container

# we can get events from the server with GetEvents command:
# -- start_timestamp (required) - start date of search range
# -- end_timestamp (required) - end date of search range
# -- parent_event - matches only events with specific parent event
# -- cache - if true, the requested data will be saved locally (default=False)

events: Data = data_source.command(commands.GetEvents(start_timestamp=START_TIME,end_timestamp=END_TIME))

# similarly we can fetch messages via GetMessages command:
# -- start_timestamp (required) - start date of search range
# -- stream (required) - this argument must be a list of either strings or Streams object.
#       We can use get_message_streams method from provider_api to find names of streams.
# -- search_direction - next or previous - sets the direction of lookup - default is next
# -- result_count_limit - Sets the maximum number of returned messages - by default it is unlimited
# -- end_timestamp - end date of search range
# -- stream_pointers - List of stream pointers to restore the search from.
#       start_timestamp will be ignored if this parameter is specified.
# -- cache - if true, the requested data will be saved locally (default=False)

s = [Streams(['newretest9'])]

messages: Data = data_source.command(commands.GetMessages(start_timestamp=START_TIME,stream=s,end_timestamp=END_TIME))


```

-The rest, for example mapping and filtering data or using event trees, is the same for now as in ["th2-data-services"](https://github.com/th2-net/th2-data-services).
