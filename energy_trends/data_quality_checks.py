import datetime
import os
from pathlib import Path
import pandas as pd

BASE_PATH = Path(__file__).parent.parent


def GetDataProfiling(data_csv_path):
    df = pd.read_csv(data_csv_path)

    stats = df.describe()
    Transposedstats = stats.T
    Transposedstats["Quarter"] = Transposedstats.index
    Transposedstats["Missingcount"] = df.isna().sum().sum()
    Transposedstats["Median"] = df.median(axis=0, skipna=True, numeric_only=True)
    print(Transposedstats.head())
    print(Transposedstats.columns)
    StatsResults = Transposedstats[['Quarter', 'Count', 'Min', 'Max', 'Mean', 'Median', 'STD']]
    return StatsResults


def Timeformatcheck(current_report_df, column_names: list, format: str = "%Y-%m-%d %H:%M:%S"):
    results = {}
    for column_name in column_names:
        ProcessedTsValues = current_report_df[column_name].unique().tolist()
        Time_format_check = "Success"
        for timestamp_value in ProcessedTsValues:
            if isinstance(datetime.datetime.strptime(timestamp_value, format), datetime.datetime) is False:
                Time_format_check = "Failed"
                break
        results.update({column_name: Time_format_check})
    return results


def GetPreviousRptColumns():
    Reports_dir = os.path.join(BASE_PATH, "reports")
    Previous_rpt_columns = []
    for file in os.listdir(Reports_dir):
        if "_data_" in file:
            continue
        else:
            df = pd.read_csv(os.path.join(Reports_dir, file))
            columns = df.columns
        Previous_rpt_columns.extend(columns)
    Unique_previous_cols = set(Previous_rpt_columns)
    return Unique_previous_cols


def MissingColsFromPrevious(current_report_df):
    Missing_cols = GetPreviousRptColumns().difference(set(current_report_df.columns))
    return Missing_cols


def NewColsFromPrevious(current_report_df):
    New_cols = set(current_report_df.columns).difference(GetPreviousRptColumns())
    return New_cols

def DataConsistencyChecks(data_csv_path):
    df = pd.read_csv(data_csv_path)
    Timeformatchecks = Timeformatcheck(df, ["processed_at"])
    Record = {column + "_" + "Time_format_check": status for column, status in Timeformatchecks.items()}
    Missing_cols = MissingColsFromPrevious(df)
    if len(Missing_cols) == 0:
        Missing_cols = ["ALL_GOOD"]
    New_cols = NewColsFromPrevious(df)
    if len(New_cols) == 0:
        New_cols = ["ALL_GOOD"]
    Record.update({"missing_columns_from_previous": "|".join(Missing_cols)})
    Record.update({"new_columns_from_previous": "|".join(New_cols)})
    Data_consistency_df = pd.DataFrame([Record])
    return Data_consistency_df
