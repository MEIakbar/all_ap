from bs4 import BeautifulSoup
import requests
from urllib.request import urlopen, urlretrieve, quote
from urllib.parse import urljoin
import pandas as pd
from tqdm import tqdm
import csv
import json
import os 


class get_dpr_tk1():

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
            "url_aceh": auth_dict['dprd_tk1']['Aceh']['url'],
            "url_sumut": auth_dict['dprd_tk1']['Sumatra Utara']['url'],
            "url_sumbar": auth_dict['dprd_tk1']['Sumatra Barat']['url'],
            "url_riau1": auth_dict['dprd_tk1']['Riau']['url_1'],
            "url_riau2": auth_dict['dprd_tk1']['Riau']['url_2'],
            "url_riau3": auth_dict['dprd_tk1']['Riau']['url_3'],
            "url_riau4": auth_dict['dprd_tk1']['Riau']['url_4'],
            "url_riau5": auth_dict['dprd_tk1']['Riau']['url_5'],
            "url_riau6": auth_dict['dprd_tk1']['Riau']['url_6'],
            "url_riau7": auth_dict['dprd_tk1']['Riau']['url_7'],
            "url_riau8": auth_dict['dprd_tk1']['Riau']['url_8'],
            "url_kepri": auth_dict['dprd_tk1']['Kepulauan Riau']['url'],
            "url_jambi": auth_dict['dprd_tk1']['Jambi']['url'],
            "url_bengkulu": auth_dict['dprd_tk1']['Bengkulu']['url'],
            "url_sumsel1": auth_dict['dprd_tk1']['Sumatra Selatan']['url_1'],
            "url_sumsel2": auth_dict['dprd_tk1']['Sumatra Selatan']['url_2'],
            "url_sumsel3": auth_dict['dprd_tk1']['Sumatra Selatan']['url_3'],
            "url_sumsel4": auth_dict['dprd_tk1']['Sumatra Selatan']['url_4'],
            "url_sumsel5": auth_dict['dprd_tk1']['Sumatra Selatan']['url_5'],
            "url_sumsel6": auth_dict['dprd_tk1']['Sumatra Selatan']['url_6'],
            "url_sumsel7": auth_dict['dprd_tk1']['Sumatra Selatan']['url_7'],
            "url_sumsel8": auth_dict['dprd_tk1']['Sumatra Selatan']['url_8'],
            "url_sumsel9": auth_dict['dprd_tk1']['Sumatra Selatan']['url_9'],
            "url_babel": auth_dict['dprd_tk1']['Kepulauan Bangka Belitung']['url'],
            "url_lampung1": auth_dict['dprd_tk1']['Lampung']['url_1'],
            "url_lampung2": auth_dict['dprd_tk1']['Lampung']['url_2'],
            "url_lampung3": auth_dict['dprd_tk1']['Lampung']['url_3'],
            "url_lampung4": auth_dict['dprd_tk1']['Lampung']['url_4'],
            "url_lampung5": auth_dict['dprd_tk1']['Lampung']['url_5'],
            "url_lampung6": auth_dict['dprd_tk1']['Lampung']['url_6'],
            "url_lampung7": auth_dict['dprd_tk1']['Lampung']['url_7'],
            "url_lampung8": auth_dict['dprd_tk1']['Lampung']['url_8'],
            "url_sulut": auth_dict['dprd_tk1']['Sulawesi Utara']['url'],
            "url_gorontalo1": auth_dict['dprd_tk1']['Gorontalo']['url_1'],
            "url_gorontalo2": auth_dict['dprd_tk1']['Gorontalo']['url_2'],
            "url_gorontalo3": auth_dict['dprd_tk1']['Gorontalo']['url_3'],
            "url_gorontalo4": auth_dict['dprd_tk1']['Gorontalo']['url_4'],
            "url_gorontalo5": auth_dict['dprd_tk1']['Gorontalo']['url_5'],
            "url_gorontalo6": auth_dict['dprd_tk1']['Gorontalo']['url_6'],
            "url_gorontalo7": auth_dict['dprd_tk1']['Gorontalo']['url_7'],
            "url_gorontalo8": auth_dict['dprd_tk1']['Gorontalo']['url_8'],
            "url_sulteng": auth_dict['dprd_tk1']['Sulawesi Tengah']['url'],
            "url_sulbar1": auth_dict['dprd_tk1']['Sulawesi Barat']['url_1'],
            "url_sulbar2": auth_dict['dprd_tk1']['Sulawesi Barat']['url_2'],
            "url_sulbar3": auth_dict['dprd_tk1']['Sulawesi Barat']['url_3'],
            "url_sulbar4": auth_dict['dprd_tk1']['Sulawesi Barat']['url_4'],
            "url_sulbar5": auth_dict['dprd_tk1']['Sulawesi Barat']['url_5'],
            "url_sulbar6": auth_dict['dprd_tk1']['Sulawesi Barat']['url_6'],
            "url_sulbar7": auth_dict['dprd_tk1']['Sulawesi Barat']['url_7'],
            "url_sulbar8": auth_dict['dprd_tk1']['Sulawesi Barat']['url_8'],
            "url_sulsel": auth_dict['dprd_tk1']['Sulawesi Selatan']['url'],
            "url_sultengga": auth_dict['dprd_tk1']['Sulawesi Tenggara']['url'],
            "url_papuabarat": auth_dict['dprd_tk1']['Papua Barat']['url'],
            "url_papua": auth_dict['dprd_tk1']['Papua']['url'],
            "url_bali": auth_dict['dprd_tk1']['Bali']['url'],
            "url_ntb": auth_dict['dprd_tk1']['Nusa Tenggara Barat']['url'],
            "url_ntt": auth_dict['dprd_tk1']['Nusa Tenggara Timur']['url'],
            "url_malukuutara": auth_dict['dprd_tk1']['Maluku Utara']['url'],
            "url_maluku": auth_dict['dprd_tk1']['Maluku']['url'],
            "url_kalbar": auth_dict['dprd_tk1']['Kalimantan Barat']['url'],
            "url_kalteng1": auth_dict['dprd_tk1']['Kalimantan Tengah']['url_1'],
            "url_kalteng2": auth_dict['dprd_tk1']['Kalimantan Tengah']['url_2'],
            "url_kalteng3": auth_dict['dprd_tk1']['Kalimantan Tengah']['url_3'],
            "url_kalteng4": auth_dict['dprd_tk1']['Kalimantan Tengah']['url_4'],
            "url_kalteng5": auth_dict['dprd_tk1']['Kalimantan Tengah']['url_5'],
            "url_kalsel1": auth_dict['dprd_tk1']['Kalimantan Selatan']['url_1'],
            "url_kalsel2": auth_dict['dprd_tk1']['Kalimantan Selatan']['url_2'],
            "url_kalsel3": auth_dict['dprd_tk1']['Kalimantan Selatan']['url_3'],
            "url_kalsel4": auth_dict['dprd_tk1']['Kalimantan Selatan']['url_4'],
            "url_kalsel5": auth_dict['dprd_tk1']['Kalimantan Selatan']['url_5'],
            "url_kaltim": auth_dict['dprd_tk1']['Kalimantan Timur']['url'],
            "url_kalut": auth_dict['dprd_tk1']['Kalimantan Utara']['url'],
            "url_jakarta1": auth_dict['dprd_tk1']['Daerah Khusus Ibukota Jakarta']['url_1'],
            "url_jakarta2": auth_dict['dprd_tk1']['Daerah Khusus Ibukota Jakarta']['url_2'],
            "url_jakarta3": auth_dict['dprd_tk1']['Daerah Khusus Ibukota Jakarta']['url_3'],
            "url_jakarta4": auth_dict['dprd_tk1']['Daerah Khusus Ibukota Jakarta']['url_4'],
            "url_jakarta5": auth_dict['dprd_tk1']['Daerah Khusus Ibukota Jakarta']['url_5'],
            "url_jakarta6": auth_dict['dprd_tk1']['Daerah Khusus Ibukota Jakarta']['url_6'],
            "url_jakarta7": auth_dict['dprd_tk1']['Daerah Khusus Ibukota Jakarta']['url_7'],
            "url_jakarta8": auth_dict['dprd_tk1']['Daerah Khusus Ibukota Jakarta']['url_8'],
            "url_jakarta9": auth_dict['dprd_tk1']['Daerah Khusus Ibukota Jakarta']['url_9'],
            "url_banten": auth_dict['dprd_tk1']['Banten']['url'],
            "url_jabar": auth_dict['dprd_tk1']['Jawa Barat']['url'],
            "url_jateng1": auth_dict['dprd_tk1']['Jawa Tengah']['url_1'],
            "url_jateng2": auth_dict['dprd_tk1']['Jawa Tengah']['url_2'],
            "url_jateng3": auth_dict['dprd_tk1']['Jawa Tengah']['url_3'],
            "url_jateng4": auth_dict['dprd_tk1']['Jawa Tengah']['url_4'],
            "url_jateng5": auth_dict['dprd_tk1']['Jawa Tengah']['url_5'],
            "url_jateng6": auth_dict['dprd_tk1']['Jawa Tengah']['url_6'],
            "url_jateng7": auth_dict['dprd_tk1']['Jawa Tengah']['url_7'],
            "url_jateng8": auth_dict['dprd_tk1']['Jawa Tengah']['url_8'],
            "url_yogya": auth_dict['dprd_tk1']['Daerah Istimewa Yogyakarta']['url'],
            "url_jatim": auth_dict['dprd_tk1']['Jawa Timur']['url']
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

    def lapung_prepro(self, x):
        x = x.replace("<p>", "")
        x = x.replace("</p>", "")
        x = x.replace("<strong>", "")
        x = x.replace("</strong>", "")
        x = x.replace("\xa0", "")
        x = x.replace("h2", "")
        x = x.replace("/h2", "")
        x = x.replace('<a href="#">', "")
        x = x.replace("/a", "")
        x = x.replace("<>", "")
        x = x.replace("</>", "")
        return x

    def get_failed_kalsel(self, url):
        soup = self.get_data(url)
        spans = soup.find_all('div', {"class":"wp-block-media-text__content"})
        ttl = spans[0].find_all("p", {"style":"font-size:15px"})[0].string
        split_ttl = ttl.split(":")[1]
        split_coma = split_ttl.split(",")
        tempat, tanggal = split_coma
        return tempat, tanggal

    def get_aceh(self):
        print("aceh....\n")
        try:
            nama_aceh = []
            soup = self.get_data(self.url_aceh)
            spans = soup.find_all('h3', {"style":"margin:0px!important"})
            for span in spans:
                nama_aceh.append(span.string)
            df = pd.DataFrame.from_dict({"nama":nama_aceh})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_aceh.csv", index=False)
                self.success_list.append("get_aceh success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_aceh failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_aceh failed..\n\n")


    def get_sumut(self):
        print("sumut....\n")
        try:
            df = pd.read_html(self.url_sumut)[10]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_sumut.csv", index=False)
                self.success_list.append("get_sumut success..")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_sumut failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_sumut failed..\n\n")


    def get_sumbar(self):
        print("sumbar....\n")
        try:
            nama_sumbar = []
            soup = self.get_data(self.url_sumbar)
            extensionsToCheck = ["1", "2", "3", "4", "5", "6", "7", "8", "9"]
            divTag = soup.find_all("div", {"align" : "justify"})
            spans = divTag[0].find_all("div")
            for span in spans[10:]:
                if any(ext in span.string for ext in extensionsToCheck):
                    nama_sumbar.append(span.string)

            nama_sumbar = [e[2:] for e in nama_sumbar]
            df = pd.DataFrame({"Nama" : nama_sumbar})
            df["Deskripsi"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_sumbar.csv", index=False)
                self.success_list.append("get_sumbar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_sumbar failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_sumbar failed..\n\n")


    def get_riau(self):
        print("riau....\n")
        try:
            nama_riau = []
            fraksi_riau = []
            urls = [self.url_riau1, self.url_riau2, self.url_riau3, self.url_riau4, 
                    self.url_riau5, self.url_riau6, self.url_riau7, self.url_riau8]
            for url in urls:
                soup = self.get_data(url) 
                spans = soup.find_all('h3', {"class":"mpc-icon-column__heading mpc-typography--mpc_preset_93 mpc-transition"})
                for span in spans:
                    nama_riau.append(span.string)
                    fraksi_riau.append(url.split("/")[-2])
            df = pd.DataFrame({"Nama" : nama_riau})
            df["Deskripsi"] = fraksi_riau
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_riau.csv", index=False)
                self.success_list.append("get_riau success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_riau failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_riau failed..\n\n")


    def get_kepri(self):
        print("kepri....\n")
        try:
            df = pd.read_html(self.url_kepri)[6]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_kepri.csv", index=False)
                self.success_list.append("get_kepri success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_kepri failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_kepri failed..\n\n")


    def get_jambi(self):
        print("jambi....\n")
        try:
            list_link = []
            soup = self.get_data(self.url_jambi)
            spans = soup.find_all('div', {"class":"home-popular-project dewan"})
            for x in spans[0].find_all('a', href=True):
                list_link.append(x['href'])
            url = list_link[-1]
            sop = self.get_data(self.url_jambi)
            list_link = list_link[:-2]
            spans = soup.find_all('div', {"class":"home-popular-project dewan"})
            for x in spans[0].find_all('a', href=True):
                list_link.append(x['href'])
            list_link = list_link[:-2]
            list_link = list(set(list_link))
            list_df = [] 
            list_name = []
            for url in tqdm(list_link[:-2]):
                try:
                    df = pd.read_html(url)[0].T
                    new_header = df.iloc[0]
                    df = df[1:]
                    df.columns = new_header
                    list_df.append(df)
                except:
                    nama = url.split("/")[-1].replace("-", " ")
                    list_name.append(nama)
            df = pd.concat(list_df)
            df2 = pd.DataFrame.from_dict({"Nama" : list_name})
            df2["Deskripsi"] = "No Data"
            df = pd.concat([df, df2]).reset_index(drop=True)
            df = df.fillna("No Data")
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_jambi.csv", index=False)
                self.success_list.append("get_jambi success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_jambi failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_jambi failed..\n\n")


    def get_bengkulu(self):
        print("bengkulu....\n")
        try:
            df = pd.read_html(self.url_bengkulu)[5]
            df["Nama"] = df["Nama Anggota"]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_bengkulu.csv", index=False)
                self.success_list.append("get_bengkulu success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_bengkulu failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_bengkulu failed..\n\n")    


    def get_sumsel(self):
        print("sumsel....\n")
        try:
            urls = [self.url_sumsel1, self.url_sumsel2, self.url_sumsel3, self.url_sumsel4, self.url_sumsel5, 
                    self.url_sumsel6, self.url_sumsel7, self.url_sumsel8, self.url_sumsel9]
            name_list = []
            deskripsi_list = []
            for url in tqdm(urls):
                soup = self.get_data(url)
                spans = soup.find_all('div', {"class":"member-name"})
                for span in spans:
                    text_list = span.text.split("\t")
                    text_list = [n.replace("\n", "") for n in text_list]
                    name_list.append(text_list[1])
                    deskripsi_list.append(text_list[2])
            df = pd.DataFrame.from_dict({"Nama" : name_list,
                                        "Deskripsi" : deskripsi_list})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_sumsel.csv", index=False)
                self.success_list.append("get_sumsel success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_sumsel failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_sumsel failed..\n\n")


    def get_babel(self):
        print("babel....\n")
        try:
            soup  = self.get_data(self.url_babel)
            df= []
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_babel.csv", index=False)
                self.success_list.append("get_bengkulu success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_bengkulu failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_bengkulu failed..\n\n")    


    def get_lampung(self):
        print("lampung....\n")
        try:
            urls = [self.url_lampung1, self.url_lampung2, self.url_lampung3, self.url_lampung4, 
                    self.url_lampung5, self.url_lampung6, self.url_lampung7, self.url_lampung8]
            list_link = []
            for url in tqdm(urls):
                soup = self.get_data(url)
                spans = soup.find_all('span', {"style":"font-size:14px"})
                for s in spans:
                    for x in s.find_all('a', href=True):
                        list_link.append(x['href'])
            Nama_list = []
            Deskripsi_list = []
            for link in tqdm(list_link):
                temp_list = []
                Nama_list.append(link.split("/")[-1])
                soup = self.get_data(link)
                spans = soup.find_all('div', {"class":"single-post-text"})            
                try:
                    for x in spans[0].find_all('p'):
                        temp_list.append(x.string)
                    Deskripsi_list.append(temp_list)
                except:
                    Deskripsi_list.append("No Data")
                    continue
            x_list = []
            x_failed = []
            for link in tqdm(list_link):
                temp_list = []
                Nama_list.append(link.split("/")[-1])
                soup = self.get_data(link)
                spans = soup.find_all('div', {"class":"single-post-text"})
                try:
                    for x in spans[0]:
                        ele_text = str(x)
                        if "\n" in ele_text:
                            continue
                        else:
                            x = self.lapung_prepro(ele_text)
                            temp_list.append(x)
                except:
                    x_failed.append(link)
                    continue
                x_list.append(temp_list)
            nama_list = []
            ttl_list = []
            for idx in x_list:
                nama_list.append(idx[0])
                ttl_list.append(idx[1])
            for child in x_failed:
                nama_list.append(child.split("/")[-1].replace("-", " "))
                ttl_list.append("-")
            df = pd.DataFrame.from_dict({"Nama" : nama_list,
                            "Tempat dan tanggal lahir" : ttl_list})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_lampung.csv", index=False)
                self.success_list.append("get_lampung success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_lampung failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_lampung failed..\n\n")

    
    def get_sulut(self):
        print("sulut....\n")
        try:
            list_nama = []
            df_list = pd.read_html(self.url_sulut) 
            for df in df_list:
                df_value = df.values.tolist()
                for data in df_value:
                    list_nama.extend(data)
            list_nama = [x for x in list_nama if str(x) != 'nan']
            df = pd.DataFrame.from_dict({"Nama":list_nama})
            df["Deskripsi"] = "No data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_sulut.csv", index=False)
                self.success_list.append("get_sulut success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_sulut failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_sulut failed..\n\n")    


    def get_gorontalo(self):
        print("gorontalo....\n")
        try:
            urls = [self.url_gorontalo1, self.url_gorontalo2, self.url_gorontalo3, self.url_gorontalo4, 
                    self.url_gorontalo5, self.url_gorontalo6, self.url_gorontalo7, self.url_gorontalo8]
            list_nama = []
            for url in urls:
                d = str(self.get_data(url)).split("figcaption")
                x = len(d)
                for idx in range(1,x,2):
                    nama = d[idx].replace(">", "")
                    nama = nama.replace("</", "")
                    list_nama.append(nama)
            df = pd.DataFrame.from_dict({"Nama":list_nama})
            df["Deskripsi"] = "No data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_gorontalo.csv", index=False)
                self.success_list.append("get_sulut success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_sulut failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_sulut failed..\n\n")


    def get_sulteng(self):
        print("sulteng....\n")
        try:
            soup = self.get_data(self.url_sulteng)
            sulawesi_tengah_nama = []
            spans = soup.find_all('span', {"class":"d-block font-gray-5 letter-spacing-1 text-uppercase font-size-12 mb-3"})
            for span in spans:
                sulawesi_tengah_nama.append(span.string)
            df = pd.DataFrame.from_dict({"Nama":sulawesi_tengah_nama})
            df["Deskripsi"] = "No data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_sulteng.csv", index=False)
                self.success_list.append("get_sulteng success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_sulteng failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_sulteng failed..\n\n")


    def get_sulbar(self):
        print("sulbar....\n")
        try:
            urls = [self.url_sulbar1, self.url_sulbar2, self.url_sulbar3, self.url_sulbar4, 
                    self.url_sulbar5, self.url_sulbar6, self.url_sulbar7]
            for url in urls:
                self.get_data(url)
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_sulbar failed..\n\n")


    def get_sulsel(self):
        print("sulsel....\n")
        try:
            li = []
            for idx in tqdm(range(81)):
                url = "https://dprd.sulselprov.go.id/web/page/anggota/{}".format(idx)
                soup = self.get_data(url)
                spans = soup.find_all('div', {"class":"row"})
                for x in spans[0].find_all('a', href=True):
                    li.append(x['href'])
            li = list(set(li))
            li_df = []
            for url in tqdm(li):
                df = pd.read_html(url)[0].T
                new_header = df.iloc[0]
                df = df[2:]
                df.columns = new_header
                li_df.append(df)
            df = pd.concat(li_df)
            df = df.rename(columns={"Nama Lengkap":"Nama"})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_sulsel.csv", index=False)
                self.success_list.append("get_sulsel success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_sulsel failed..\n\n")    
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_sulsel failed..\n\n")


    def get_sultengara(self):
        print("sulawesi tenggara....\n")
        try:
            df = pd.read_html(self.get_sultengara)[6]
            if df.shape[0] > 5:
                self.success_list.append("get_sulsel success..\n")
                df.to_csv("./scrapping/result/dprd_tk1/dprd_sulawesi_tenggara.csv", index=False)
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_sulsel failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_sulbar failed..\n\n")


    def get_papuabarat(self):
        print("papua barat....\n")
        try:
            df = pd.read_html(self.url_papuabarat)[5]
            df = df.rename(columns={"Nama Anggota":"Nama"})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_papuabarat.csv", index=False)
                self.success_list.append("get_papuabarat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_papuabarat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_papuabarat failed..\n\n")


    def get_papua(self):
        print("papua....\n")
        try:
            soup = self.get_data(self.url_papua)
            list_nama = []
            d = str(soup).split("figcaption")
            x = len(d)
            for idx in range(1,x,2):
                    nama = d[idx].replace(">", "")
                    nama = nama.replace("</", "")
                    text =  'class="vc_figure-caption"'
                    nama = nama.replace(text, "")
                    list_nama.append(nama)
            df = pd.DataFrame.from_dict({"Nama" : list_nama})
            df["Deskripsi"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_papua.csv", index=False)
                self.success_list.append("get_papua success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_papua failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_papua failed..\n\n")


    def get_bali(self):
        print("bali....\n")
        try:
            df = pd.read_html(self.url_bali)[6]
            df = df.rename(columns={"Nama Anggota":"Nama"})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_bali.csv", index=False)
                self.success_list.append("get_bali success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_bali failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_bali failed..\n\n")


    def get_ntb(self):
        print("NTB....\n")
        try:
            df = pd.read_html(self.url_ntb)[8]
            df = df.rename(columns={"Nama Anggota":"Nama"})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_ntb.csv", index=False)
                self.success_list.append("get_ntb success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_ntb failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_ntb failed..\n\n")


    def get_ntt(self):
        print("NTT....\n")
        try:
            df = pd.read_html(self.url_ntt)[8]
            df = df.rename(columns={"Nama Anggota":"Nama"})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_ntt.csv", index=False)
                self.success_list.append("get_ntt success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_ntt failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_ntt failed..\n\n")


    def get_malukuutara(self):
        print("maluku utara....\n")
        try:
            df = pd.read_html(self.url_malukuutara)[5]
            df = df.rename(columns={"Nama Anggota":"Nama"})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_maluku_utara.csv", index=False)
                self.success_list.append("get_malukuutara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_malukuutara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_malukuutara failed..\n\n")


    def get_maluku(self):
        print("maluku....\n")
        try:
            df = pd.read_html(self.url_maluku)[2][1:]
            df = df.rename(columns={"Anggota":"Nama"})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_maluku.csv", index=False)
                self.success_list.append("get_maluku success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_maluku failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_maluku failed..\n\n")


    def get_kalbar(self):
        print("kalimantan barat....\n")
        try:
            df = pd.read_html(self.url_kalbar)[2][1:]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_kalbar.csv", index=False)
                self.success_list.append("get_kalbar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_kalbar failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)            
            self.failed_list.append("get_kalbar failed..\n\n")

    
    def get_kalteng(self):
        print("kalimantan tengah....\n")
        try:
            urls = [self.url_kalteng1, self.url_kalteng2, self.url_kalteng3, self.url_kalteng4, self.url_kalteng5]
            list_df = []
            for url in urls:
                list_df.append(pd.read_html(url))
            a_list = []
            b_list = []
            for df_idx in list_df:
                for i in range(0,len(df_idx)-1,2):
                    a_list.append(df_idx[i])
                for i in range(1,len(df_idx)-1,2):
                    b_list.append(df_idx[i])

            df_nama = pd.concat(a_list)
            df_nama = df_nama.rename(columns={1 : "Nama"})
            df_nama = df_nama[["Nama"]].reset_index(drop=True)
            df_posisi = pd.concat(b_list)
            df_posisi = df_posisi.loc[0]
            df_posisi = df_posisi.rename(columns={1 : "Jabatan"})
            df_posisi = df_posisi[["Jabatan"]].reset_index(drop=True)
            df = pd.merge(df_nama, df_posisi, left_index=True, right_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_kalteng.csv", index=False)
                self.success_list.append("get_kalteng success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_kalteng failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_kalteng failed..\n\n")


    def get_kalsel(self):
        try:
            urls = [self.url_kalsel1, self.url_kalsel2, self.url_kalsel3, self.url_kalsel4, self.url_kalsel5]
            list_link = []
            for url in tqdm(urls):
                soup = self.get_data(url)
                spans = soup.find_all('div', {"class":"entry-content clearfix"})
                for x in spans[0].find_all('a', href=True):
                    list_link.append(x['href']) 
            list_nama = []
            list_tempat = []
            list_tanggal = []
            list_failed = []
            for idx in tqdm(range(len(list_link))):
                url = list_link[idx]
                nama = url.split("/")[-2]   
                nama = nama.replace("-", " ")
                try:
                    df_sample = pd.read_html(url)[0]
                    split_coma = df_sample[1][0].split(",")
                    tempat_lahir = split_coma[0]
                    tangal_lahir = split_coma[1].split(" ")[1]
                    list_nama.append(nama)
                    list_tempat.append(tempat_lahir)
                    list_tanggal.append(tangal_lahir)
                except:
                    list_failed.append(url)
            failed_again = []
            for idx in range(len(list_failed)):
                url = list_failed[idx]
                try:
                    if idx == 0 or idx == 17:
                        nama = url.split("/")[-1]
                    else:
                        nama = url.split("/")[-2]  
                    nama = nama.replace("-", " ")
                    tempat, tanggal = self.get_failed_kalsel(url)
                    list_nama.append(nama)
                    list_tempat.append(tempat_lahir)
                    list_tanggal.append(tangal_lahir)
                except:
                    failed_again.append(url)
            df = pd.DataFrame.from_dict({"Nama": list_nama,
                            "tempat lahir": list_tempat,
                            "tanggal lahir": list_tanggal})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_kalsel.csv", index=False)
                self.success_list.append("get_kalsel success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_kalsel failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_kalsel failed..\n\n")


    def get_kaltim(self):
        print("kalimantan timur....\n")
        try:
            soup = self.get_data(self.url_kaltim)
            list_link = []
            spans = soup.select('div', {"class":"row justify-content-center"})
            for x in spans[0].find_all('a', href=True):
                list_link.append(x['href'])    
            list_link = list_link[48:103]
            list_nama = []
            list_lahir = []
            for idx in tqdm(range(len(list_link))):
                url = list_link[idx]
                nama = url.split("/")[-1].replace("-", " ")
                soup = self.get_data(url)
                spans = soup.find_all('h6', {"class":"text-black text-bold-500"})
                lahir = spans[0].text
                list_nama.append(nama)
                list_lahir.append(lahir)
            df = pd.DataFrame.from_dict({"Nama" : list_nama,
                            "lahir" : list_lahir})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_kaltim.csv", index=False)
                self.success_list.append("get_kaltim success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_kaltim failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_kaltim failed..\n\n")


    def get_kalut(self):
        print("kalimantan utara....\n")
        try:
            df = pd.read_html(self.url_kalut)[6]
            df = df.rename(columns={"Nama Anggota":"Nama"})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_kalut.csv", index=False)
                self.success_list.append("get_kalut success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_kalut failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_kalut failed..\n\n")


    def get_jakarta(self):
        print("jakarta....\n")
        try:
            Base_url = "https://dprd-dkijakartaprov.go.id"
            urls = [self.url_jakarta1, self.url_jakarta2, self.url_jakarta3, self.url_jakarta4, self.url_jakarta5, 
                    self.url_jakarta6, self.url_jakarta7, self.url_jakarta8, self.url_jakarta9]
            list_link = []
            for url in urls:
                soup = self.get_data(url)
                spans = soup.select('div', {"class":"innerpage-right"})
                for x in spans:
                    for x2 in x.find_all('a', href=True):
                        if "http" in x2['href']:
                            continue
                        elif x2['href'] == '/' or x2['href'] == '':
                            continue
                        else:
                            list_link.append(Base_url + x2['href'])  
            list_link = list(set(list_link))
            list_nama = []
            list_lahir = []
            for idx in tqdm(range(len(list_link))):
                url = list_link[idx]
                nama = url.split("/")[-2].replace("-", " ")
                soup = self.get_data(url)
                spans = soup.find_all('div', {"class":"col-md-8"})
                for span in spans:
                    lahir = span.find_all('p')[0].text.split("\n")[0].replace("\xa0", "")                
                list_nama.append(nama)
                list_lahir.append(lahir)
            df = pd.DataFrame.from_dict({"Nama" : list_nama,
                            "list_lahir" : list_lahir})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_jakarta.csv", index=False)
                self.success_list.append("get_jakarta success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_jakarta failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_jakarta failed..\n\n")


    def get_banten(self):
        print("banten....\n")
        try:
            df = []
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_banten.csv", index=False)
                self.success_list.append("get_banten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_banten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_banten failed..\n\n")


    def get_jabar(self):
        print("jawa barat....\n")
        try:
            Base_url = "https://dprd.jabarprov.go.id/profil/"
            soup = self.get_data(self.url_jabar)
            list_link = []
            spans = soup.find_all('a', {"class":"d-inline-block text-center link-dark"}, href=True)
            for span in spans:
                list_link.append(span['href'])
            nama_list = []
            nama_tempat = []
            nama_tanggal = []
            nama_alamat = []
            for urlx in tqdm(list_link):
                soup = self.get_data(urlx)
                temp_list = []
                for span in soup.find_all("div", {"class":"fw-300"}):
                    temp_list.append(span.string)
                nama_list.append(urlx.split("/")[-1].replace("-", " "))
                nama_tempat.append(temp_list[0])
                nama_tanggal.append(temp_list[1])
                nama_alamat.append(temp_list[2])
            df = pd.DataFrame.from_dict({"Nama" : nama_list,
                             "tempat lahir" : nama_tempat,
                             "tanggal lahir" : nama_tanggal,
                             "alamat" : nama_alamat})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_jabar.csv", index=False)
                self.success_list.append("get_jabar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_jabar failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_jabar failed..\n\n")


    def get_jateng(self):
        print("jawa tengah....\n")
        try:
            urls = [self.url_jateng1, self.url_jateng2, self.url_jateng3, self.url_jateng4, 
                    self.url_jateng5, self.url_jateng6, self.url_jateng7, self.url_jateng8]
            nama_list = []
            for url in urls:
                len_gambar = 0
                soup = self.get_data(url)
                spans = soup.find_all('div', {"class":"vc_single_image-wrapper vc_box_border vc_box_border_grey"})
                for span in spans :
                    title_text = span.find('img', alt=True).get("title")
                    nama_list.append(title_text)
                    len_gambar +=1
            df = pd.DataFrame({"Nama" : nama_list})
            df["Deskripsi"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_jateng.csv", index=False)
                self.success_list.append("get_jateng success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_jateng failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_jateng failed..\n\n")


    def get_yogya(self):
        print("yogyakarta....\n")
        try:
            df = pd.read_html(self.url_yogya)[0]
            nama_list = df[df[2].str.contains("NAMA")][4].tolist()
            daerah_list = df[df[2].str.contains("DAERAH PEMILIHAN")][4].tolist()
            parpol_list = df[df[2].str.contains("PARPOL")][4].tolist()

            df = pd.DataFrame.from_dict({"Nama" : nama_list,
                                        "DAERAH PEMILIHAN" : daerah_list,
                                        "PARPOL" : parpol_list})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_yogya.csv", index=False)
                self.success_list.append("get_yogya success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_yogya failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_yogya failed..\n\n")


    def get_jatim(self):
        print("jawa timur....\n")
        try:
            soup = self.get_data(self.url_jatim)
            nama_list = []
            for span in soup.find_all("h3", {"class" : "team-name"}):
                nama_list.append(span.string)
            nama_list = list(set(nama_list))
            df = pd.DataFrame({"Nama" : nama_list})
            df["Deskripsi"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk1/dprd_jatim.csv", index=False)
                self.success_list.append("get_jatim success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("get_jatim failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("get_jatim failed..\n\n")


    def get_dprd1(self):
        # writepath = './log_scrapping.txt'
        self.get_aceh()
        self.get_sumut()    
        self.get_sumbar()
        self.get_riau()
        self.get_kepri()
        self.get_jambi()
        self.get_bengkulu()
        self.get_sumsel()
        self.get_babel()
        self.get_lampung()
        self.get_sulut()
        self.get_gorontalo()
        self.get_sulteng()
        self.get_sulbar()
        self.get_sulsel()
        self.get_sultengara()
        self.get_papuabarat()
        self.get_papua()
        self.get_bali()
        self.get_ntb()
        self.get_ntt()
        self.get_malukuutara()
        self.get_maluku()
        self.get_kalbar()
        self.get_kalteng()
        self.get_kalsel()
        self.get_kaltim()
        self.get_kalut()
        self.get_jakarta()
        self.get_banten()
        self.get_jabar()
        self.get_jateng()
        self.get_yogya()
        self.get_jatim()
        
        df = pd.DataFrame.from_dict({"status": self.failed_list,
                                    "error desc": self.error_desc})
        df.to_excel("./log_scrapping.xlsx", index=False)
        # mode = 'a' if os.path.exists(writepath) else 'r+'
        # with open(writepath, mode) as f:
        #     f.write('Sucess list\n')
        #     sc = str(self.success_list)
        #     f.write(sc)
        #     f.write('\n Failed list\n')
        #     fl = str(self.failed_list)
        #     f.write(fl)
