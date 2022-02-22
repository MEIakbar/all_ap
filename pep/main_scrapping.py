import pandas as pd
import numpy as np
import os
import logging
import schedule
import datetime
import time
from scrapping.scrapping_kabinet import get_kabinet
from scrapping.scrapping_dpr_mpr import get_dpr_mpr
from scrapping.scrapping_wagub import get_gub_wagub
from scrapping.scrapping_dprd_tk1 import get_dpr_tk1
from scrapping.scrapping_dprd_tk2 import get_dpr_tk2
from glob import glob

import warnings
warnings.filterwarnings("ignore")


cols_names = ["Nama", "tempat lahir", "tanggal lahir", "Remove Gelar Depan", "only_character"]
filter_cols = ["Nama", "tempat lahir", "tanggal lahir"]
path_kabinet = "./scrapping/result/kabinet/"
path_dpr_mpr = "./scrapping/result/dpr_mpr/"
path_gubernur = "./scrapping/result/gubernur/"
path_dprd1 = "./scrapping/result/dprd_tk1/"
path_dprd2 = "./scrapping/result/dprd_tk2/"


def data_list(path_folder):
    EXT = "*.csv"
    all_csv_files = [file
                    for path, subdir, files in os.walk(path_folder)
                    for file in glob(os.path.join(path, EXT))]
    return all_csv_files

def prepro_nama(df_nama):
    df_nama["Nama"] = df_nama["Nama"].str.rjust(1, " ")
    df_nama["Nama"] = df_nama["Nama"].str.replace(",", " ")
    df_nama["Nama"] = df_nama["Nama"].str.replace(".", " ")
    df_nama["Nama"] = df_nama["Nama"].str.replace("(", "")
    df_nama["Nama"] = df_nama["Nama"].str.replace(")", "")
    df_nama['Nama'] = ' ' + df_nama['Nama'].astype(str)
    df_nama['Nama'] = df_nama['Nama'].astype(str) + " "
    list_filter = [" s h i ", " s h m kn ", " stp ", " s pd i ", " s sos i ", " sh i ", " s psi ", " 2019-2020 ", " belum diusulkan ", " belum ditetapkan ", " h ", " s pd ", " sh ",
                    " mh ", " se ", " si kom ", " m kom ", " s ag ", " ir ", " s si ", " m si ", " s ip ", " mm ", " ba ", " m ag ", " apt ", " s a p ", " sm hk ", " s ab ",
                    " s i kom ", " m sc ", " drs ", " hj ", " kh ", " m m ", " sp m ", " mayjen tni purn ", " stp ", " s tp ", " s kom ", " lc ", " skm ", " ss ", " mt ",
                    " dr ", " ak mm ", " s sos ", " s sos i ", " sp og ", " se ", " m ec dev ", " s ap ", " msi ", " skom ", " spsi ", " bcomm ",
                  " s m m si ", " s sos i ", "  ll ciarb ", " s p ", " cpa ma ", " st ", ":", " am kep ", " s pd i ", " s ikom ", " m hum ",
                  " s ap ", " m pd ", "<br/ketua dprd", "<br/wakil ketua 2", "<br/wakil ketua 1", "<br/wakil ketua 3 ",
                  " msi  ak ", " b comm ", " lc ma ", " b buss ", " dra ", " a md ", " s h", " spd " ]
    filter_str = '|'.join(list_filter)
    df_nama['Remove Gelar Depan'] = df_nama['Nama'].str.replace(filter_str, ' ')
    df_nama["only_character"] = df_nama["Remove Gelar Depan"].str.replace('[^a-zA-Z]', '')
    return df_nama


def data_cleaning(df, col):
    dict_date = {"Jan" : "/01/", "Feb" : "/02/", "Mar" : "/03/", "Apr" : "/04/", "May" : "/05/", "Jun" : "/06/",
                "Jul" : "/07/", "Aug" : "/08/", "Sep" : "/09/", "Oct" : "/10/", "Nov" : "/11/", "Des" : "/12/",
                "Januari" : "/01/", "Februari" : "/02/", "Maret" : "/03/", "April" : "/04/", "Mei" : "/05/",
                "Juni" : "/06/", "July" : "/07/", "Agustus" : "/08/", "September" : "/09/", "Oktober" : "/10/",
                "November" : "/11/", "Nopember" : "/11/", "Desember" : "/12/", "januari" : "/01/", "februari" : "/02/", "maret" : "/03/", 
                "april" : "/04/", "mei" : "/05/", "juni" : "/06/", "july" : "/07/", "agustus" : "/08/", "september" : "/09/", 
                "oktober" : "/10/", "november" : "/11/", "nopember" : "/11/", "desember" : "/12/"}
    dic_value = {r"\b{}\b".format(k): v for k, v in dict_date.items()}
    df[col] = df[col].replace(dic_value, regex=True)
    return df


def prepro_kabinet():
    print("Preprocessing Kabinet")
    all_csv_files = data_list(path_kabinet)
    list_df = [pd.read_csv(x) for x in all_csv_files]
    df_kabinet = pd.concat(list_df, ignore_index=True)
    df_kabinet = df_kabinet.reset_index(drop=True)
    df_kabinet["tanggal lahir"] = [x[0] for x in df_kabinet["Lahir"].str.split("umur")]
    df_kabinet["tempat lahir"] = [x[1] if len(x) >1 else x[0] for x in df_kabinet["Lahir"].str.split("umur")]
    df_kabinet["tempat lahir"] = df_kabinet["tempat lahir"].str.replace('[^a-zA-Z]', '')
    df_kabinet =  data_cleaning(df_kabinet, "tanggal lahir")
    df_kabinet["tanggal lahir"] = df_kabinet["tanggal lahir"].str.replace('[^0-9//]', '')
    df_kabinet = df_kabinet[filter_cols]
    return df_kabinet


def prepro_dpr_mpr():
    print("Preprocessing DPR MPR RI")
    all_csv_files = data_list(path_dpr_mpr)
    list_df = [pd.read_csv(x) for x in all_csv_files]
    df_dpr_mpr = pd.concat(list_df, ignore_index=True)
    df_dpr_mpr = df_dpr_mpr.reset_index(drop=True)
    df_dpr_mpr = df_dpr_mpr.drop_duplicates(subset=["Nama"], keep="first").reset_index(drop=True)
    df_dpr_mpr["Tempat Lahir / Tgl Lahir"] = df_dpr_mpr["Tempat Lahir / Tgl Lahir"].fillna("No Data")
    df_dpr_mpr["tanggal lahir"] = [x[1] if len(x) >1 else "00/00/0000" for x in df_dpr_mpr["Tempat Lahir / Tgl Lahir"].str.split("/")]
    df_dpr_mpr["tempat lahir"] = [x[0] if len(x) >1 else np.nan for x in df_dpr_mpr["Tempat Lahir / Tgl Lahir"].str.split("/")]
    df_dpr_mpr = df_dpr_mpr[filter_cols]
    df_dpr_mpr =  data_cleaning(df_dpr_mpr, "tanggal lahir")
    return df_dpr_mpr


def prepro_gubernur():
    print("Preprocessing gubernur")
    all_csv_files = data_list(path_gubernur)
    df_gubernur = pd.read_csv(all_csv_files[0])
    df_gubernur["tanggal lahir"] = [x[0] for x in df_gubernur["Lahir"].str.split("umur")]
    df_gubernur["tempat lahir"] = [x[1] if len(x) >1 else x[0] for x in df_gubernur["Lahir"].str.split("umur")]
    df_gubernur["tempat lahir"] = df_gubernur["tempat lahir"].str.replace('[^a-zA-Z/, ]', '')
    df_gubernur =  data_cleaning(df_gubernur, "tanggal lahir")
    df_gubernur["tanggal lahir"] = df_gubernur["tanggal lahir"].str.replace('[^0-9//]', '')
    df_gubernur = df_gubernur.rename(columns={"nama":"Nama"})
    df_gubernur = df_gubernur[filter_cols]
    return df_gubernur


def prepro_dprd1():
    print("Preprocessing DPRD Tingkat 1")
    all_csv_files = data_list(path_dprd1)
    list_df = [pd.read_csv(x) for x in all_csv_files]
    df_dprd_tk1 = pd.concat(list_df, ignore_index=True)
    df_dprd_tk1 = df_dprd_tk1.reset_index(drop=True)
    df_dprd_tk1 = df_dprd_tk1[['Nama', 'tempat lahir', 'tanggal lahir', 'list_lahir', 'Tempat dan tanggal lahir']]
    df_dprd_tk1["Tempat dan tanggal lahir"] = [(x).split("Tempat/tgl. lahir :")[-1] if type(x) == str else x for x in df_dprd_tk1["Tempat dan tanggal lahir"]]
    df_dprd_tk1["list_lahir"] = [(x).split("Tempat/tgl. lahir:")[-1] if type(x) == str else x for x in df_dprd_tk1["list_lahir"]]
    df_dprd_tk1["Tempat lahir 1"] = [x.split(",")[0] if type(x) == str else x for x in df_dprd_tk1["Tempat dan tanggal lahir"]]
    df_dprd_tk1["Tanggal lahir 1"] = [x.split(",")[-1] if type(x) == str else x for x in df_dprd_tk1["Tempat dan tanggal lahir"]]
    df_dprd_tk1["Tempat lahir 2"] = [x.split(",")[0] if type(x) == str else x for x in df_dprd_tk1["list_lahir"]]
    df_dprd_tk1["Tanggal lahir 2"] = [x.split(",")[-1] if type(x) == str else x for x in df_dprd_tk1["list_lahir"]]
    df_dprd_tk1['tempat lahir'] = df_dprd_tk1['tempat lahir'].combine_first(df_dprd_tk1['Tempat lahir 1'])
    df_dprd_tk1['tempat lahir'] = df_dprd_tk1['tempat lahir'].combine_first(df_dprd_tk1['Tempat lahir 2'])
    df_dprd_tk1['tanggal lahir'] = df_dprd_tk1['tanggal lahir'].combine_first(df_dprd_tk1['Tanggal lahir 1'])
    df_dprd_tk1['tanggal lahir'] = df_dprd_tk1['tanggal lahir'].combine_first(df_dprd_tk1['Tanggal lahir 2'])
    df_dprd_tk1 = data_cleaning(df_dprd_tk1, "tanggal lahir")
    df_dprd_tk1 = df_dprd_tk1[filter_cols]
    return df_dprd_tk1


def prepro_dprd2():
    print("Preprocessing DPRD Tingkat 2")
    all_csv_files = data_list(path_dprd2)
    list_df = []
    for file in all_csv_files:
        try:
            df = pd.read_csv(file)
            list_df.append(df)
        except:
            print(file)
    df_dprd_tk_II = pd.concat(list_df, ignore_index=True)
    df_dprd_tk_II = df_dprd_tk_II[df_dprd_tk_II["nama"].notnull()].reset_index(drop = True)
    df_dprd_tk_II = df_dprd_tk_II.rename(columns={"nama" : "Nama"})
    df_dprd_tk_II["Nama"] = df_dprd_tk_II["Nama"].str.lower()
    df_dprd_tk_II = df_dprd_tk_II[filter_cols]
    return df_dprd_tk_II


def combine_all_data():
    list_df = [prepro_kabinet(), prepro_dpr_mpr(), prepro_gubernur(), prepro_dprd1(), prepro_dprd2()]
    df = pd.concat(list_df, ignore_index=True)
    df = prepro_nama(df)
    time_now = "{:%Y_%m_%d}".format(datetime.datetime.now())
    df.to_csv("./data/all_pep_data_{}.csv".format(time_now))


def job():
    print("Time... : ")
    print(datetime.datetime.now().time())
    print('Load Config... ')
    json_path = "./scrapping/config.json"
    json_path2 = "./scrapping/config_tk2.json"
    scrp_kabinet = get_kabinet.load_config_json(json_path)
    scrp_dpr_mpr = get_dpr_mpr.load_config_json(json_path)
    scrp_gub = get_gub_wagub.load_config_json(json_path)
    scrp_dprd1 = get_dpr_tk1.load_config_json(json_path)
    scrp_dprd2 = get_dpr_tk2.load_config_json(json_path2)


    print('Scrapping Data... ')
    start_time = datetime.datetime.now()
    print("start date and time :", start_time)	
    scrp_kabinet.get_kabinet_data()
    scrp_dpr_mpr.get_dpr_data()
    scrp_dprd1.get_dprd1()
    scrp_dprd2.get_all_data()
    print("Preprocessing Proccess")
    combine_all_data()
    end_time = datetime.datetime.now()
    print("start date and time :", start_time)
    print("End :", end_time)


# schedule.every(90).minutes.do(job)
schedule.every(7200).hour.do(job)


while True:
    schedule.run_pending()
    time.sleep(60) # wait one minute