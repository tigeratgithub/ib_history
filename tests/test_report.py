from ib_history.report import FetchReport


def test_report_to_dict():
    report = FetchReport(symbols=["MNQ"], bars=["1m"], ranges=[{"start": "a", "end": "b"}])
    data = report.to_dict()
    assert data["symbols"] == ["MNQ"]
    assert data["bars"] == ["1m"]
