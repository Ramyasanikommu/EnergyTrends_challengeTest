import datetime
import os
from pathlib import Path
import pandas as pd

BASE_PATH = Path(__file__).parent.parent


def get_data_profiling(data_csv_path):
    df = pd.read_csv(data_csv_path)

    stats = df.describe()
    transposed_stats = stats.T
    transposed_stats["Quarter"] = transposed_stats.index
    transposed_stats["missing_count"] = df.isna().sum().sum()
    transposed_stats["median"] = df.median(axis=0, skipna=True, numeric_only=True)
    print(transposed_stats.head())
    print(transposed_stats.columns)
    stats_results = transposed_stats[['Quarter', 'count', 'min', 'max', 'mean', 'median', 'std']]
    return stats_results


def timeformat_check(current_report_df, column_names: list, format: str = "%Y-%m-%d %H:%M:%S"):
    results = {}
    for column_name in column_names:
        processed_ts_values = current_report_df[column_name].unique().tolist()
        time_format_check = "Success"
        for timestamp_value in processed_ts_values:
            if isinstance(datetime.datetime.strptime(timestamp_value, format), datetime.datetime) is False:
                time_format_check = "Failed"
                break
        results.update({column_name: time_format_check})
    return results


def get_previous_rpt_columns():
    reports_dir = os.path.join(BASE_PATH, "reports")
    previous_rpt_columns = []
    for file in os.listdir(reports_dir):
        if "_data_" in file:
            continue
        else:
            df = pd.read_csv(os.path.join(reports_dir, file))
            columns = df.columns
        previous_rpt_columns.extend(columns)
    unique_previous_cols = set(previous_rpt_columns)
    return unique_previous_cols


def missing_cols_from_previous(current_report_df):
    missing_cols = get_previous_rpt_columns().difference(set(current_report_df.columns))
    return missing_cols


def new_cols_from_previous(current_report_df):
    new_cols = set(current_report_df.columns).difference(get_previous_rpt_columns())
    return new_cols

def data_consistency_checks(data_csv_path):
    df = pd.read_csv(data_csv_path)
    timeformat_checks = timeformat_check(df, ["processed_at"])
    record = {column + "_" + "time_format_check": status for column, status in timeformat_checks.items()}
    missing_cols = missing_cols_from_previous(df)
    if len(missing_cols) == 0:
        missing_cols = ["ALL_GOOD"]
    new_cols = new_cols_from_previous(df)
    if len(new_cols) == 0:
        new_cols = ["ALL_GOOD"]
    record.update({"missing_columns_from_previous": "|".join(missing_cols)})
    record.update({"new_columns_from_previous": "|".join(new_cols)})
    data_consistency_df = pd.DataFrame([record])
    return data_consistency_df
