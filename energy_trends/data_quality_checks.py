import datetime
import os
from pathlib import Path
import pandas as pd

BASE_PATH = Path(__file__).parent.parent


def Get_Data_Profiling(data_csv_path):
    df = pd.read_csv(data_csv_path)

    Stats = df.describe()
    Transposed_Stats = Stats.T
    Transposed_Stats["Quarter"] = Transposed_Stats.index
    Transposed_Stats["Missing_Count"] = df.isna().sum().sum()
    Transposed_Stats["Median"] = df.median(axis=0, skipna=True, numeric_only=True)
    Stats_Results = Transposed_Stats[['Quarter', 'Count', 'Min', 'Max', 'Mean', 'Median', 'Std']]
    return Stats_Results


def Timeformat_Check(Current_Report_df, column_names: list, format: str = "%Y-%m-%d %H:%M:%S"):
    Results = {}
    for column_name in column_names:
         Processed_Ts_Values = Current_Report_df[column_name].unique().tolist()
         Time_Format_Check = "Success"
        for timestamp_value in  Processed_Ts_Values:
            if isinstance(datetime.datetime.strptime(timestamp_value, format), datetime.datetime) is False:
                 Time_Format_Check = "Failed"
                break
        Results.update({column_name:  Time_Format_Check})
    return Results


def  Get_Previous_Rpt_Columns():
    Reports_Dir = os.path.join(BASE_PATH, "reports")
    Previous_Rpt_Columns = []
    for file in os.listdir(Reports_Dir):
        if "_data_" in file:
            continue
        else:
            df = pd.read_csv(os.path.join(Reports_Dir, file))
            columns = df.columns
        Previous_Rpt_Columns.extend(columns)
    Unique_Previous_Cols = set(Previous_Rpt_Columns)
    return Unique_Previous_Cols


def Missing_cols_From_Previous(Current_Report_df):
    Missing_cols =  Get_Previous_Rpt_Columns().difference(set(Current_Report_df.columns))
    return Missing_cols


def New_cols_from_Previous(Current_Report_df):
    New_cols = set(Current_Report_df.columns).difference( Get_Previous_Rpt_Columns())
    return New_cols

def Data_Consistency_Checks(data_csv_path):
    df = pd.read_csv(data_csv_path)
    Timeformat_Checks = timeformat_check(df, ["processed_at"])
    Record = {column + "_" + " Time_Format_Check": status for column, status in Timeformat_Checks.items()}
    Missing_cols = Missing_cols_From_Previous(df)
    if len(Missing_cols) == 0:
        Missing_cols = ["ALL_GOOD"]
    New_cols = New_cols_from_Previous(df)
    if len(New_cols) == 0:
        New_cols = ["ALL_GOOD"]
    Record.update({"missing_columns_from_previous": "|".join(Missing_cols)})
    Record.update({"new_columns_from_previous": "|".join(New_cols)})
    Data_Consistency_df = pd.DataFrame([Record])
    return Data_Consistency_df
