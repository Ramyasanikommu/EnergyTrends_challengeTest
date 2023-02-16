import pandas as pd
from unittest.mock import patch

from energy_trends.data_quality_checks import timeformat_check, missing_cols_from_previous, new_cols_from_previous

consistent_timeformat_records = [{"id": 1234, "processed_at": "2023-02-16 13:45:23"},
                                     {"id": 1345, "processed_at": "2023-02-17 13:50:40"}]
inconsistent_timeformat_records = [{"id": 1234, "processed_at": "2023-02-16 13:45:23"},
                                       {"id": 1345, "processed_at": "2023/02/17T13:50:40"}]

def test_timeformat_check():

    df = pd.DataFrame(consistent_timeformat_records)
    actual = timeformat_check(df, ["processed_at"])
    assert actual == {'processed_at': 'Success'}
    df = pd.DataFrame(inconsistent_timeformat_records)
    actual = timeformat_check(df, ["processed_at"])
    assert actual == {'processed_at': 'Failed'}


def test_missing_and_new_columns():
    new_df = pd.DataFrame(consistent_timeformat_records)
    with patch("energy_trends.data_quality_checks.get_previous_rpt_columns") as prev_cols:
        prev_cols.return_value = {"id", "processed_at", "status"}
        actual = missing_cols_from_previous(new_df)
        assert list(actual) == ["status"]

        prev_cols.return_value = {"id"}
        actual = new_cols_from_previous(new_df)
        assert list(actual) == ["processed_at"]