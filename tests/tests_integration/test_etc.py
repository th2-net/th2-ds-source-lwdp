from th2_data_services.data import Data
from th2_data_services.event_tree import EventTreeCollection

from th2_data_services.data_source.lwdp.data_source import DataSource
from th2_data_services.data_source.lwdp.event_tree import ETCDriver

from tests.tests_integration.conftest import DataCase


def test_etc(all_events: DataCase, http_data_source: DataSource):
    """Just check, that we can create ETC using our driver."""
    events: Data[dict] = all_events.data
    etc = EventTreeCollection(driver=ETCDriver(http_data_source))
    etc.build(events)
    etc.recover_unknown_events()
    assert 21 == len(etc)
    assert 1 == len(etc.get_trees())
