from th2_data_services_lwdp.filters.filter import LwDPFilter


def test_filter_url():
    """Old style filters."""
    filter_ = LwDPFilter("type", ["one", 2, "three"], False, False)
    assert (
        filter_.url()
        == "&filters=type&type-values=one&type-values=2&type-values=three&type-negative=False&type-conjunct=False"
    )

    filter_ = LwDPFilter("name", "one", False, False)
    assert filter_.url() == "&filters=name&name-values=one&name-negative=False&name-conjunct=False"

    filter_ = LwDPFilter("name", 1)
    assert filter_.url() == "&filters=name&name-values=1&name-negative=False&name-conjunct=False"


def test_filter_grcp():
    assert isinstance(LwDPFilter("type", ["one", 2, "three"], False, False), LwDPFilter)


def test_iterate_filter_twice():
    f = LwDPFilter("type", ["one", 2, "three"])
    v1 = f.url()
    v2 = f.url()
    assert v1 == v2
