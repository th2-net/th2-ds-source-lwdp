import pytest
import th2_data_services

from th2_data_services_lwdp.adapters.adapter_sse import get_default_sse_adapter


def test_interactive_mode_errors_without_command(dummy_error_events):
    th2_data_services.INTERACTIVE_MODE = True
    adapter = get_default_sse_adapter()
    adapter.data_link = dummy_error_events
    events = dummy_error_events.map_stream(adapter)
    assert events.limit(0)  # For 'errors2' To Be Recorded

    # for _ in events: pass # Object will get iterated and metadata['errors2'] *WILL NOT* gets doubled.

    events.metadata["errors2"] = adapter.interactive_mode_errors
    assert events.metadata.get("errors") is None
    # print(">>> Items:", len(events.metadata['errors2']))
    assert len(events.metadata["errors2"]) == dummy_error_events.len


def test_interactive_mode_errors_without_command_with_different_assert(dummy_error_events):
    th2_data_services.INTERACTIVE_MODE = True
    events = dummy_error_events
    adapter = get_default_sse_adapter()
    adapter.data_link = events
    events = events.map_stream(adapter)
    assert events.filter(lambda x: x)  # For 'errors2' To Be Recorded
    events.metadata["errors2"] = adapter.interactive_mode_errors
    assert events.metadata.get("errors") is None
    assert len(events.metadata["errors2"]) == dummy_error_events.len


@pytest.mark.skip(reason="Not 100% accurate tests, since errors events arent actually present.")
def test_interactive_mode_errors_with_command(events_from_demo_book_1):
    th2_data_services.INTERACTIVE_MODE = True
    events = events_from_demo_book_1
    assert events.metadata.get("errors") is None
    assert events.metadata["errors2"] == []
