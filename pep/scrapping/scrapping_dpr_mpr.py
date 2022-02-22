from bs4 import BeautifulSoup
import requests
from urllib.request import urlopen, urlretrieve, quote
from urllib.parse import urljoin
import pandas as pd
from tqdm import tqdm
import csv
import json


class get_dpr_mpr():

    def __init__(self, all_config_dict):
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
            "url": auth_dict['dpr']['url'],
            "Base_url": auth_dict['dpr']['Base_url']
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


    def get_data(self, url):
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
        }
        r = requests.get(url, headers=headers)  # Using the custom headers we defined above
        soup = BeautifulSoup(r.content, 'html5lib') 
        return soup


    def get_des(self, url):
        soup = self.get_data(url)
        
        list_field = []
        for j, field in enumerate(soup.find_all("div", {"class" : "ket-title pull-left"})):
            if j == 0:
                continue
            else:
                list_field.append(field.text)
            
        list_inp = []
        for k, inp in enumerate(soup.find_all("div", {"class" : "input pull-left"})):
            if k == 0:
                continue
            else:
                list_inp.append(inp.text.replace(": ", ""))
        
        result = dict(zip(list_field, list_inp))
        # add nama
        for a in soup.find_all("h3", {"class":"text-center"}):
            result["Nama"] = a.text

        return result

    def get_mpr(self, url, Base_url):
        try:
            li = []
            for idx in tqdm(range(37)):
                url = "https://www.mpr.go.id/keanggotaan/anggota-mpr-ri?page={}".format(idx+1)
                li.append(pd.read_html(url)[0])

            df_MPR = pd.concat(li, axis=0, ignore_index=True)
            df_MPR = df_MPR.drop_duplicates().reset_index(drop=True)

            cols = list(df_MPR.columns)
            cols.remove("Nama Anggota")
            df_MPR['Deskripsi'] = df_MPR[cols].values.tolist()
            df_MPR['Nama'] = df_MPR['Nama Anggota']
            df_MPR = df_MPR[["Nama", "Deskripsi"]]
            
            print("get_mpr success..")
            return df_MPR
        except:
            print("get_mpr failed..")


    def get_dpr(self, url, Base_url):
        try:
            soup = self.get_data(url)
            li = []
            for x in soup.find_all("tr", {"style" : "cursor:pointer;"}):
                pejabat = x['onclick']
                link_profile = pejabat.split("href=")[-1].replace("'", "")
                li.append(Base_url + link_profile)
            

            list_failed = []
            list_success = []
            for idx in tqdm(range(len(li))):
                try:
                    result = self.get_des(li[idx])
                    list_success.append(result)
                except:
                    list_failed.append(li[idx])

            df_dpr = pd.DataFrame(list_success)
            df_dpr = df_dpr.drop_duplicates().reset_index(drop=True)

            print("get_dpr success..")
            return df_dpr
        except:
            print("get_dpr failed..")


    def get_dpr_data(self):
        df_mpr = self.get_mpr(self.url, self.Base_url)
        df_mpr.to_csv("./scrapping/result/dpr_mpr/mpr.csv", index=False)

        df_dpr = self.get_dpr(self.url, self.Base_url)
        df_dpr.to_csv("./scrapping/result/dpr_mpr/dpr.csv", index=False)


