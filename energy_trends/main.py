from bs4 import BeautifulSoup
import requests
import os
from pathlib import Path
import pandas as pd
import datetime

BASE_PATH = Path(__file__).parent.parent
DATA_DIR = os.path.join(BASE_PATH, "source_data")

from energy_trends.data_quality_checks import get_data_profiling, data_consistency_checks


class EnergyTrends:
    def __init__(self, web_url: str, reports_dir_path: str):
        self.reports_dir_path = reports_dir_path
        self.web_url = web_url

    def _web_scrapper(self):
        response = requests.get(self.web_url)
        if response.status_code == 200:
            web_content = response.text
            soup = BeautifulSoup(web_content, 'html.parser')
            return soup
        else:
            response.raise_for_status()

    def is_file_already_downloaded(self, file_name: str):
        existing_files = []
        for existing_file in os.listdir(DATA_DIR):
            existing_file = existing_file.split("/")[-1]
            if "ET_3.1" in existing_file and existing_file.endswith(".xlsx"):
                existing_files.append(existing_file)

        if file_name in existing_files:
            return True
        else:
            return False

    def download_supply_use_data(self, file_url: str):
        response = requests.get(file_url)
        file_name = file_url.split("/")[-1]
        file_path = os.path.join(DATA_DIR, file_name)
        with open(file_path, "wb") as f:
            f.write(response.content)
        print("Supply & Use of crudeoil, gas and feedstocks file:" + file_name + " is successfully downloaded")
        return file_path

    def extract_source_data_link_from_website(self):
        # attachment_7159263 corresponds to Supply and use of crude oil, natural gas liquids, and feedstocks
        soup = self._web_scrapper()
        sect = soup.find("section", class_="attachment embedded", id="attachment_7159263")
        match = sect.find(name="div")
        file_link = match.a.get('href')
        print("WebScrapping of the url is completed and source_data url extracted: " + file_link)
        return file_link

    def read_supply_use_quarter_data(self, excel_file_path: str):
        # The first 4 rows contain heading and description, the source_data header columns are in row 5
        df_quarter = pd.read_excel(excel_file_path, header=4, sheet_name="Quarter")
        df_quarter = df_quarter.rename(columns={'Column1': 'Product'})
        df_quarter["processed_at"] = datetime.datetime.now()
        df_quarter["FileName"]=excel_file_path.split("/")[-1]
        df_quarter=df_quarter.fillna('',inplace=True)
        print("Quarter sheet source_data is successfully parsed for: " + excel_file_path)
        return df_quarter

    def df_to_csv(self, df, file_path):
        df.to_csv(file_path, encoding='utf-8', index=False, date_format="%Y-%m-%d %H:%M:%S")

    def create_csv_reports(self, quarter_data: pd.DataFrame, file_name_prefix: str):
        headers = [column.strip().replace(" ", "_").replace("\n", "") for column in quarter_data.columns]
        quarter_data.columns = headers
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = file_name_prefix + "_" + timestamp_str + ".csv"
        data_csv_file_path = os.path.join(self.reports_dir_path, file_name)
        self.df_to_csv(quarter_data, data_csv_file_path)
        print("Quarter data CSV Report is created successfully")

        profiling_file_name = file_name_prefix + "_data_profiling_" + timestamp_str +".csv"
        profiling_data = get_data_profiling(data_csv_file_path)
        self.df_to_csv(profiling_data, os.path.join(self.reports_dir_path, profiling_file_name))
        print("Data Profiling report is created successfully")

        data_consistency_report = file_name_prefix + "_data_consistency_" + timestamp_str + ".csv"
        data_consistency_data = data_consistency_checks(data_csv_file_path)
        self.df_to_csv(data_consistency_data, os.path.join(self.reports_dir_path, data_consistency_report))
        print("Data Consistency report is created successfully")

    def run(self):
        supply_use_file_link = self.extract_source_data_link_from_website()
        latest_file_name = supply_use_file_link.split("/")[-1]
        if self.is_file_already_downloaded(latest_file_name) is False:
            latest_file_path = self.download_supply_use_data(supply_use_file_link)
            quarter_data = self.read_supply_use_quarter_data(latest_file_path)
            self.create_csv_reports(quarter_data, latest_file_name.replace(".xlsx", ""))
        else:
            print("The file: " + latest_file_name + " is already downloaded earlier and hence aborting processing")


if __name__ == '__main__':
    web_url = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"
    reports_dir = os.path.join(BASE_PATH, "reports")
    energy_trends = EnergyTrends(web_url, reports_dir)
    energy_trends.run()

