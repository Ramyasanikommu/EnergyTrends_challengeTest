from bs4 import BeautifulSoup
import requests
import os
from pathlib import Path
import pandas as pd
import datetime

BASE_PATH = Path(__file__).parent.parent
DATA_DIR = os.path.join(BASE_PATH, "source_data")

from energy_trends.data_quality_checks import Get_Data_Profiling, Data_Consistency_Checks


class EnergyTrends:
    def __init__(self, Web_url: str, Reports_dir_path: str):
        self.Reports_dir_path = Reports_dir_path
        self.Web_url = Web_url

    def _Web_Scrapper(self):
        Response = requests.get(self.Web_url)
        if Response.status_code == 200:
            Web_content = Response.text
            Soup = BeautifulSoup(Web_content, 'html.parser')
            return Soup
        else:
            Response.raise_for_status()

    def Is_File_already_downloaded(self, File_name: str):
         Existing_files = []
        for Existing_file in os.listdir(DATA_DIR):
            Existing_file = Existing_file.split("/")[-1]
            if "ET_3.1" in Existing_file and Existing_file.endswith(".xlsx"):
                 Existing_files.append(Existing_file)

        if File_name in  Existing_files:
            return True
        else:
            return False

    def Download_Supply_use_data(self, file_url: str):
        Response = requests.get(file_url)
        File_name = file_url.split("/")[-1]
        File_Path = os.path.join(DATA_DIR, File_name)
        with open(File_Path, "wb") as f:
            f.write(Response.content)
        print("Supply & Use of crudeoil, gas and feedstocks file:" + File_name + " is successfully downloaded")
        return File_Path

    def Extract_Source_Data_link_from_website(self):
        # Here to extract the Supply and use of crude oil, natural gas liquids, and feedstocks related data we inspect the website link and from there 
        #we get the attachment embedded class name with respect to the link id attachment_715963
        Soup = self._Web_Scrapper()
        Sect = Soup.find("section", class_="attachment embedded", id="attachment_7159263")
        Match = Sect.find(name="div")
        File_Link = Match.a.get('href')
        print("WebScrapping of the url is completed and source_data url extracted: " + File_Link)
        return File_Link

    def  Read_Supply_use_quarter_data(self, excel_File_Path: str):
        # using the file link from the website we read the interested Quater data from the xlsx file and then analysing the data and added the processed timestamp and respected file name required.
        Df_Quarter = pd.read_excel(excel_File_Path, header=4, sheet_name="Quarter")
        Df_Quarter = Df_Quarter.rename(columns={'Column1': 'Product'})
        Df_Quarter["processed_at"] = datetime.datetime.now()
        Df_Quarter["FileName"]=excel_File_Path.split("/")[-1]
        Df_Quarter=Df_Quarter.fillna('',inplace=True)
        print("Quarter sheet source_data is successfully parsed for: " + excel_File_Path)
        return Df_Quarter

    def Df_to_CSV(self, df, File_Path):
        df.to_csv(File_Path, encoding='utf-8', index=False, date_format="%Y-%m-%d %H:%M:%S")
        
        
# In this section i have created the 3 kinds reports 1.Data report, 2. Data Profiling and 3. Data Consistency with the file name and respect to timestamp in the .csv fomat 
    def Create_csv_reports(self, Quarter_Data: pd.DataFrame, File_name_prefix: str):
        Headers = [column.strip().replace(" ", "_").replace("\n", "") for column in Quarter_Data.columns]
        Quarter_Data.columns = Headers
        Timestamp_Str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        File_name = File_name_prefix + "_" + Timestamp_Str + ".csv"
        Data_csv_File_Path = os.path.join(self.Reports_dir_path, File_name)
        self.Df_to_CSV(quarter_data, Data_csv_File_Path)
        print("Quarter data CSV Report is created successfully")

        Profiling_File_name = File_name_prefix + "_data_profiling_" + Timestamp_Str +".csv"
        Profiling_data = Get_Data_Profiling(Data_csv_File_Path)
        self.Df_to_CSV(Profiling_data, os.path.join(self.Reports_dir_path, Profiling_File_name))
        print("Data Profiling report is created successfully")

        Data_consistency_report = File_name_prefix + "_data_consistency_" + Timestamp_Str + ".csv"
        Data_consistency_data = Data_Consistency_Checks(Data_csv_File_Path)
        self.Df_to_CSV(Data_consistency_data, os.path.join(self.Reports_dir_path, Data_consistency_report))
        print("Data Consistency report is created successfully")

    def run(self):
        # This is the main method which triggers the whole process and also checks where the file was downloaded or not if yes it will print already file was downloaded else it will print all the 3 types of reports. 
        Supply_use_file_link = self.Extract_Source_Data_link_from_website()
        Latest_File_name = Supply_use_file_link.split("/")[-1]
        if self. Is_File_already_downloaded(Latest_File_name) is False:
            Latest_File_Path = self.Download_Supply_use_data(Supply_use_file_link)
            Quarter_Data = self. Read_Supply_use_quarter_data(Latest_File_Path)
            self.Create_csv_reports(quarter_data, Latest_File_name.replace(".xlsx", ""))
        else:
            print("The file: " + Latest_File_name + " is already downloaded earlier and hence aborting processing")


if __name__ == '__main__':
    Web_url = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"
    Reports_dir = os.path.join(BASE_PATH, "reports")
    Energy_trends = EnergyTrends(Web_url, Reports_dir)
    Energy_trends.run()

