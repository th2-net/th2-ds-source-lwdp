# 1. Introduction

This repository is a library for creating th2-ds-source-lwdp applications.

The library used to analyze stream data using _aggregate operations_ mainly from
the ["Lightweight Data Provider"](https://github.com/th2-net/th2-lw-data-provider). Data Services allows you to manipulate
the stream data processing workflow using _pipelining_.

The library allows you:

- Natively connect to ["Lightweight Data Provider"](https://github.com/th2-net/th2-lw-data-provider) via
  `GRPCDataSource` class and extract TH2 Events or Messages via _commands_
- Build Event Trees (`EventsTreeCollection` class)

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

### GRPC provider warning
This library has ability to interact with several versions of grpc providers, but it's limited by installed version of
`th2_grpc_data_provider` package version. You can use only appropriate version of provider api, which is compatible with
installed version of `th2_grpc_data_provider`.

By default, `th2_data_services` uses the latest available version of provider api version.

#### Reasons for the restriction
1. Two different versions of `th2_grpc_data_provider` can't be installed in the same virtual environment;
2. Two different versions of package `th2_grpc_data_provider` may depend on different versions of packages `th2_grpc_common`;
3. In the case of using another package in the process of using `th2_data_services` (for example `th2_common`), 
which also depends on `th2_grpc_common`, a version conflict may occur (both at the Python level and at the Protobuf level).


#### Switch to another interface
The transition to another version of the interface is carried out by installing another version of the 
`th2_grpc_data_provider` package by running this command (in case you are using pip):
    
    pip install th2_grpc_data_provider==<version>

For example, if you want to use v5 provider api, then you should run this command:
    
    pip install th2_grpc_data_provider==0.1.6


#### Versions compatibility table

| Provider api | th2_grpc_data_provider version |
|:------------:|:------------------------------:|
|      v5      |             0.1.6              |
|      v6      |             1.1.0              |


## 2.2. Example

A good, short example is worth a thousand words.

This example works with **Events**, but you also can do the same actions with **Messages**.

[The following example as a file](examples/get_started_example.py).

<!-- start get_started_example.py -->
```python
from collections import Generator
from typing import Tuple, List, Optional
from datetime import datetime

from th2_data_services import Data
from th2_data_services.events_tree import EventsTree
from th2_data_services.provider.v5.data_source.http import HTTPProvider5DataSource
from th2_data_services.provider.v5.commands import http as commands
from th2_data_services.provider.v5.events_tree import EventsTreeCollectionProvider5, ParentEventsTreeCollectionProvider5
from th2_data_services.provider.v5.filters.event_filters import NameFilter, TypeFilter, FailedStatusFilter
from th2_data_services.provider.v5.filters.message_filters import BodyFilter

# [0] Lib configuration
# [0.1] Interactive or Script mode
# If you use the lib in interactive mode (jupyter, ipython) it's recommended to set the special
# global parameter to True. It'll keep cache files if something went wrong.
import th2_data_services

th2_data_services.INTERACTIVE_MODE = True

# [0.2] Logging
# Import helper function to setup logging.
from th2_data_services import add_stderr_logger, add_file_logger

add_stderr_logger()  # Just execute it to activate DS lib logging. Debug level by default.
# or if you want to put logs to the file
add_file_logger()

# [1] Create DataSource object to connect to rpt-data-provider.
DEMO_HOST = "10.100.66.66"  # th2-kube-demo  Host port where rpt-data-provider is located.
DEMO_PORT = "30999"  # Node port of rpt-data-provider.
data_source = HTTPProvider5DataSource(f"http://{DEMO_HOST}:{DEMO_PORT}")

START_TIME = datetime(
    year=2021, month=6, day=17, hour=9, minute=44, second=41, microsecond=692724
)  # Datetime in utc format.
END_TIME = datetime(year=2021, month=6, day=17, hour=12, minute=45, second=50)

# [2] Get events or messages from START_TIME to END_TIME.
# [2.1] Get events.
events: Data = data_source.command(
    commands.GetEvents(
        start_timestamp=START_TIME,
        end_timestamp=END_TIME,
        attached_messages=True,
        # Use Filter class to apply rpt-data-provider filters.
        # Do not use multiple classes of the same type.
        filters=[
            TypeFilter("Send message"),
            NameFilter(["ExecutionReport", "NewOrderSingle"]),  # You can use multiple values.
            FailedStatusFilter(),
        ],
    )
)
