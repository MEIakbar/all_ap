from bs4 import BeautifulSoup
import requests
from urllib.request import urlopen, urlretrieve, quote
from urllib.parse import urljoin
import pandas as pd
from tqdm import tqdm
import csv
import json
import os 


class get_gub_wagub():

    def __init__(self, all_config_dict):
        self.error_desc= []
        self.failed_list = []
        self.success_list = []
        for key in all_config_dict:
            setattr(self, key, all_config_dict[key])

    @staticmethod
    def parse_config(auth_dict):
        """
        input function: selecting parameters from input file
        parameter required :
        1. type [required]: the file type must be .json

        :return: dictionary
        """
        list_dict_config = []
        all_config_dict = {
            "url": auth_dict['gubernur']['url'],
            "base_url": auth_dict['gubernur']['BASE_URL']
        }
        list_dict_config.append(all_config_dict)
        return list_dict_config


    @staticmethod
    def load_config(json_path):
        """
        load Config from JSON file
        """
        f = open(json_path)
        json_config = json.load(f)

        return json_config


    @classmethod
    def load_config_json(cls, json_path):
        auth_json = cls.load_config(json_path)

        return cls(*cls.parse_config(auth_json))


    def get_url(self, url):
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
        }
        r = requests.get(url, headers=headers)  # Using the custom headers we defined above
        soup = BeautifulSoup(r.content, 'html5lib') 
        return soup


    def get_list_urls(self):
        soup = self.get_url(self.url).find("table")

        gubernur_list = []
        gubernur_names = []
        wagub_list = []
        wagub_names= []
        for tag in soup.select("td:nth-of-type(4) a"):
            gubernur_list.append(self.base_url + tag["href"])
            gubernur_names.append(tag["title"])
        for tag in soup.select("td:nth-of-type(7) a"):
            wagub_list.append(self.base_url + tag["href"])
            wagub_names.append(tag["title"])

        return gubernur_list, gubernur_names, wagub_list, wagub_names

    
    def get_gubernur(self, gubernur_list, gubernur_names):
        list_gub_s =[]
        print("Scrapping Gubernur....")
        for idx in tqdm(range(len(gubernur_list))):
            try:
                df = pd.read_html(gubernur_list[idx])[0].T
                df.columns = df.iloc[0]
                df = df.iloc[[1]]
                df["nama"] = gubernur_names[idx]
                df = df[["nama","Lahir"]].reset_index(drop=True)
                list_gub_s.append(df)
            except:
                df = pd.read_html(gubernur_list[idx])[1].T
                df.columns = df.iloc[0]
                df = df.iloc[[1]]
                df.index.name = None
                df["nama"] = gubernur_names[idx]
                df = df[["nama","Lahir"]].reset_index(drop=True)
                list_gub_s.append(df)
        
        return list_gub_s


    def get_wagub(self, wagub_list, wagub_names):
        list_wagub_s =[]
        list_wagub_g =[]
        print("Scrapping Wagub....")
        for idx in tqdm(range(len(wagub_list))):
            try:
                df = pd.read_html(wagub_list[idx])[0].T
                df.columns = df.iloc[0]
                df = df.iloc[[1]]
                df["nama"] = wagub_names[idx]
                df = df[["nama","Lahir"]].reset_index(drop=True)
                list_wagub_s.append(df)
            except:
                list_wagub_g.append(idx)

        return list_wagub_s


    def get_dataframe(self):

        gubernur_list, gubernur_names, wagub_list, wagub_names = self.get_list_urls()
        gub = self.get_gubernur(gubernur_list, gubernur_names)
        df_gub = pd.concat(gub, axis=0).reset_index(drop=True)

        wagub = self.get_wagub(wagub_list, wagub_names)
        df_wagub = pd.concat(wagub, axis=0).reset_index(drop=True)

        df_info = pd.DataFrame.from_dict({"jumlah gubernur": [df_gub.shape[0]],
                       "jumlah wakil gubernur": [df_wagub.shape[0]]})
        df_info.to_excel("./gub_dan_wagub_log.xlsx", index=False)

        df = pd.concat([df_gub, df_wagub], axis=0).reset_index(drop=True)
        df.to_csv("./scrapping/result/gubernur/gubernur_wagub.csv", index=False)





