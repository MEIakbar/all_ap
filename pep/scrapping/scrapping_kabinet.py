from bs4 import BeautifulSoup
import requests
from urllib.request import urlopen, urlretrieve, quote
from urllib.parse import urljoin
import pandas as pd
from tqdm import tqdm
import csv
import json


class get_kabinet():

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
            "url": auth_dict['kabinet']['url'],
            "BASE_URL": auth_dict['kabinet']['BASE_URL']
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
    

    def get_wiki(self, url):
        df = pd.read_html(url)[0].T
        new_header = df.iloc[0] #grab the first row for the header
        df = df[1:] #take the data less the header row
        df.columns = new_header #set the header row as the df header
        df = df.reset_index()
        df = df.rename(columns={"index" : "Nama"})
        df = df[["Nama", "Lahir"]]
        return df

    
    def get_wiki_failed(self, idx, url):
        df = pd.read_html(url)[idx].T
        new_header = df.iloc[0] #grab the first row for the header
        df = df[1:] #take the data less the header row
        df.columns = new_header #set the header row as the df header
        df = df.reset_index()
        df = df.rename(columns={"index" : "Nama"})
        df = df[["Nama", "Lahir"]]
        return df

    def get_wiki_failed_2(self, idx, url):
        if idx == 2:
            df = pd.read_html(url)[idx].T
        else:
            df = pd.read_html(url)[0].T
        new_header = df.iloc[0] #grab the first row for the header
        df = df[1:] #take the data less the header row
        df.columns = new_header #set the header row as the df header
        df = df.reset_index()
        df = df.rename(columns={"index" : "Nama"})
        df = df[["Nama", "Lahir"]]
        return df
    
    def get_pimpinan(self, url, BASE_URL):
        try:
            links_content = []
            soup = self.get_data(url)
            table = soup.find('table', 'wikitable')
            links = table.findAll('a')
            for idx, link in enumerate(links):
                if idx == 0 or idx == 3:
                    links_content.append(BASE_URL + link['href'])
            
            li = []
            for idx in tqdm(range(len(links_content))):
                df = self.get_wiki(links_content[idx])
                li.append(df)
            df = pd.concat(li).reset_index(drop=True)
            df["Nama"] = df["Nama"].str.replace(".1", "")

            print("get_pimpinan success..")
            return df
        except:
            print("get_pimpinan failed..")


    def get_menteri(self, url, BASE_URL):
        try:
            df = pd.read_html(url)[5]
            df.columns = df.columns.droplevel(1)
            df = df.loc[:,~df.columns.duplicated(keep='last')]
            df = df.reset_index(drop=True) 
            df = df[df["Pejabat"] != "Menteri"].reset_index(drop=True)

            list_link_menteri = []
            for name in list(df["Pejabat"]):
                name = name.replace(" ", "_")
                list_link_menteri.append(BASE_URL + "wiki/" + name)

            list_sukses = []
            list_gagal = []
            li = []
            for link in tqdm(list_link_menteri):
                try:
                    df_sample = self.get_wiki(link)
                    df_sample["Pejabat"] = link.split("/")[-1].replace("_", " ")
                    li.append(df_sample)
                    list_sukses.append(link)
                except:
                    list_gagal.append(link)

            new_link = ["https://id.wikipedia.org/wiki/Mahfud_MD",
                        "https://id.wikipedia.org/wiki/Ida_Fauziyah",
                        "https://id.wikipedia.org/wiki/Luhut_Binsar_Panjaitan"]
            list_pejabat_g = ["https://id.wikipedia.org/wiki/Mohammad_Mahfud_MD",
                            "https://id.wikipedia.org/wiki/Ida_Fauziyah",
                            "https://id.wikipedia.org/wiki/Luhut_Binsar_Panjaitan"]
            
            for idx in range(len(new_link)):
                df_failed = self.get_wiki_failed(idx, new_link[idx])
                df_failed["Pejabat"] = list_pejabat_g[idx].split("/")[-1].replace("_", " ")
                li.append(df_failed)
            df_lahir = pd.concat(li).reset_index(drop=True)
            df_menteri = pd.merge(df, df_lahir, on="Pejabat")
            print("get_menteri success..")
            return df_menteri
        except:
            print("get_menteri failed..")


    def get_pejabat(self, url, BASE_URL):
        try:
            df_3 = pd.read_html(url)[7]
            df_3 = df_3.drop(["No.", "Pejabat"], axis=1)
            df_3 = df_3.rename(columns={"Pejabat.1" : "Pejabat"})

            list_link_pejabat = []
            for name in list(df_3["Pejabat"]):
                name = name.replace(" ", "_")
                list_link_pejabat.append(BASE_URL + "wiki/" + name)
            
            list_sukses = []
            list_gagal = []
            li = []
            for link in tqdm(list_link_pejabat):
                try:
                    df_sample = self.get_wiki(link)
                    df_sample["Pejabat"] = link.split("/")[-1].replace("_", " ")
                    li.append(df_sample)
                    list_sukses.append(link)
                except:
                    list_gagal.append(link)

            new_link = ["https://id.wikipedia.org/wiki/Arminsyah",
                        "https://id.wikipedia.org/wiki/Ari_Dono_Sukmanto",
                        "https://id.wikipedia.org/wiki/Laksana_Tri_Handoko"]
            list_pejabat_g = ['https://id.wikipedia.org/wiki/Arminsyah(Pelaksana_tugas)',
                            'https://id.wikipedia.org/wiki/Ari_Dono_Sukmanto(Pelaksana_tugas)',
                            'https://id.wikipedia.org/wiki/Laksana_Tri_Handoko']

            for idx in range(len(new_link)):
                df_failed = self.get_wiki_failed_2(idx, new_link[idx])
                df_failed["Pejabat"] = list_pejabat_g[idx].split("/")[-1].replace("_", " ")
                li.append(df_failed)
                df3_lahir = pd.concat(li).reset_index(drop=True)
                df_pejabat = pd.merge(df_3, df3_lahir, on="Pejabat")
            
            print("get_pejabat success..")
            return df_pejabat
        except:
            print("get_pejabat failed..")


    def get_wakil_menteri(self, url, BASE_URL):
        try:
            df_4 = pd.read_html(url)[8]
            df_4 = df_4.drop(["No.", "Pejabat"], axis=1)
            df_4 = df_4.rename(columns={"Pejabat.1" : "Pejabat"})

            list_link_wamen = []
            for name in list(df_4["Pejabat"]):
                name = name.replace(" ", "_")
                list_link_wamen.append(BASE_URL + "wiki/" + name)
            
            list_sukses = []
            list_gagal = []
            li = []
            for link in tqdm(list_link_wamen):
                try:
                    df_sample = self.get_wiki(link)
                    df_sample["Pejabat"] = link.split("/")[-1].replace("_", " ")
                    li.append(df_sample)
                    list_sukses.append(link)
                except:
                    list_gagal.append(link)
            
            new_link = ["https://id.wikipedia.org/wiki/Wempi_Wetipo"]
            list_pejabat_g = ['https://id.wikipedia.org/wiki/John_Wempi_Wetipo']

            df_sample = self.get_wiki(new_link[0])
            df_sample["Pejabat"] = list_pejabat_g[0].split("/")[-1].replace("_", " ")
            li.append(df_sample)
            df4_lahir = pd.concat(li).reset_index(drop=True)
            df_wamen = pd.merge(df_4, df4_lahir, on="Pejabat")

            print("get_wakil_menteri success..")
            return df_wamen
        except:
            print("get_wakil_menteri failed..")


    def get_kabinet_data(self):
        df_pimpinan = self.get_pimpinan(self.url, self.BASE_URL)
        df_pimpinan.to_csv("./scrapping/result/kabinet/kabinet_pimpinan.csv", index=False)

        df_menteri = self.get_menteri(self.url, self.BASE_URL)
        df_menteri.to_csv("./scrapping/result/kabinet/kabinet_menteri.csv", index=False)

        df_pejabat = self.get_pejabat(self.url, self.BASE_URL)
        df_pejabat.to_csv("./scrapping/result/kabinet/kabinet_pejabat.csv", index=False)
        
        df_wamen = self.get_wakil_menteri(self.url, self.BASE_URL)        
        df_wamen.to_csv("./scrapping/result/kabinet/kabinet_wamen.csv", index=False)












