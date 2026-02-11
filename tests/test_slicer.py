from datetime import datetime

from ib_history.slicer import slice_range


def test_slice_range_basic():
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 10)
    slices = slice_range(start, end, max_days=3)
    assert len(slices) == 3
    assert slices[0].start == start
    assert slices[-1].end == end
