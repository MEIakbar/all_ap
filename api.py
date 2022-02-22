import time
import json
import logging
import uvicorn
import pandas as pd
from ast import literal_eval
from pydantic import BaseModel
from typing import Optional
from dttot.service.utility import get_similarity
from datetime import datetime
from math import floor, ceil
import re
from scraparazzie import scraparazzie
from bs4 import BeautifulSoup
import requests

import secrets
from fastapi import Depends, FastAPI, BackgroundTasks, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

import warnings
warnings.filterwarnings("ignore")

app = FastAPI()
security = HTTPBasic()

class Userinput(BaseModel):
    Nama: str
    NIK: Optional[str]=None
    DOB: Optional[str]=None
    POB: Optional[str]=None

def dttot_get_constraint():
    """
    get constraint / schema data for DTTOT
    output : DataFrame
    """
    file_path = "./dttot/data/Constraint_PPATK.csv"
    df = pd.read_csv(file_path)
    return df

def dttot_get_all_data():
    """
    get data
    output : DataFrame
    """
    df = pd.read_csv("./dttot/data/all_data.csv")
    df['nama_list'] = df['nama_list'].apply(literal_eval)
    df = df.fillna('no data')
    return df

def dttot_get_input_char(df, nama):
    """
    filter nama column based on the first 4 character for each word
    output : DataFrame
    """
    input_char = ''.join([i[:4] for i in nama.strip().split(' ')])
    df = df[df["4_char"].str.contains(input_char)].reset_index(drop=True)
    return df

def dttot_DOB_similarity(df, col, dob_input):
    """
    filter DOB column
    output : DataFrame
    """
    df = df[df[col].str.contains(dob_input)].reset_index(drop=True)
    return df

def dttot_NIK_similarity(df, col, NIK_input):
    """
    filter NIK column
    output : DataFrame
    """
    df = df[df[col].str.contains(NIK_input)].reset_index(drop=True)
    return df

def dttot_POB_similarity(df, col, pob_input):
    """
    filter POB column
    output : DataFrame
    """
    try:
        df = df[df[col].str.contains(pob_input)].reset_index(drop=True)
    except:
        df = df[df[col].str.contains(pob_input).fillna(False)].reset_index(drop=True)
    return df

def dttot_nama_similarity(df, input_nama, treshold_value):
    """
    get similarity value for nama column
    output : DataFrame
    """
    df = get_similarity(df, input_nama, treshold_value)
    return df

def to_json(df):
    return df.to_json(orient='records')

def dttot_treatment_constraint(nama_status, nik_status, dob_status, pob_status):
    df = dttot_get_constraint()
    dict_value = {"nama" :nama_status,
                "nik" : nik_status,
                "dob" : dob_status,
                "pob" : pob_status}
    result = df.loc[(df[list(dict_value)] == pd.Series(dict_value)).all(axis=1)]
    result_recommendation =  list(set(result["recommendation"]))[0]
    return result_recommendation

# def dttot_get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
#     correct_username = secrets.compare_digest(credentials.username, "app")
#     correct_password = secrets.compare_digest(credentials.password, "mnc123456")
#     if not (correct_username and correct_password):
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Incorrect email or password",
#             headers={"WWW-Authenticate": "Basic"},
#         )
#     return credentials.username

def dttot_main_funct(Nama, NIK, DOB, POB):
    # if the user fill with empty string trun it into None
    if Nama is not None:
        if Nama.isspace() or Nama == "":
            Nama = None
    if NIK is not None:
        if NIK.isspace() or NIK == "":
            NIK = None
    if DOB is not None:
        if DOB.isspace() or DOB == "":
            DOB = None
    if POB is not None:
        if POB.isspace() or POB == "":
            POB = None
    # initialization some variable
    nama_status = "not match"
    nik_status = "not match"
    dob_status = "not match"
    pob_status = "not match"
    alamat_status = "not match"
    Similarity_Percentage = 0.8
    dict_filter = {}

    # get data
    print("Getting Data...")
    start_time = time.time()
    df = dttot_get_all_data()
    print("--- %s seconds ---" % (time.time() - start_time))

    print("filter name...")
    start_time = time.time()
    # filter nama berdasarkan 4 character awal untuk setiap kata
    if Nama is not None:
        Nama = Nama.strip()
        Nama = Nama.lower()
        df_nama = dttot_get_input_char(df, Nama)
        if df_nama.shape[0] > 0:
            df = df_nama.copy()
            dict_filter["nama"] = Nama
            nama_status = "match"
    print("--- %s seconds ---" % (time.time() - start_time))
    # filter NIK_input
    print("Filter NIK...")
    start_time = time.time()
    if NIK is not None:
        NIK = NIK.strip()
        NIK = NIK.lower()
        print(df.shape)
        if df_nama.shape[0] > 0:
            df = df_nama.copy()
        print(df.shape)
        df_NIK = dttot_NIK_similarity(df, 'nik', NIK)
        if df_NIK.shape[0] > 0:
            df = df_NIK.copy()
            dict_filter["nik"] = NIK
            if len(NIK) <= 14:
                nik_status = "not match"
            else:
                nik_status = "match"
    print("--- %s seconds ---" % (time.time() - start_time))
    # filter DOB_similarity
    print("Filter DOB...")
    start_time = time.time()
    if DOB is not None:
        DOB = DOB.strip()
        DOB = DOB.lower()
        df_DOB = dttot_DOB_similarity(df_nama, 'tanggal lahir', DOB)
        if NIK is not None:
            if df_NIK.shape[0] > 0:
                df_DOB = dttot_DOB_similarity(df_NIK, 'tanggal lahir', DOB)
        if df_DOB.shape[0] > 0:
            df_nama = df_DOB.copy()
            dob_status = "match"
    print("--- %s seconds ---" % (time.time() - start_time))
    # filter POB_similarity
    print("Filter POB...")
    start_time = time.time()
    if POB is not None:
        POB = POB.strip()
        POB = POB.lower()
        df_POB = dttot_POB_similarity(df_nama, 'tempat lahir', POB)
        if NIK is not None:
            if df_NIK.shape[0] > 0:
                df_POB = dttot_POB_similarity(df_NIK, 'tempat lahir', POB)
        if df_POB.shape[0] > 0 :
            df = df_POB.copy()
            dict_filter["pob"] = POB
            pob_status = "match"
    print("--- %s seconds ---" % (time.time() - start_time))
    # set Note output
    statusList = [nama_status, nik_status, dob_status, pob_status, alamat_status]
    if 'match' in (statusList):
        df_outp = df.copy()
        cols = ["nama", "nama_list", "nik", "tanggal lahir", "tempat lahir", "kewarganegaraan", "paspor", "alamat"]
        df_outp = df_outp[cols]
        outp = to_json(df_outp)
    else:
        outp = None
    # get Similarity_Score
    simalarity_value = None
    if nama_status == "match":
        df = dttot_nama_similarity(df, Nama, Similarity_Percentage)
        simalarity_value = df["similarity"][0]
        if simalarity_value < 0.8:
            nama_status = "not match"
    reccomendation = dttot_treatment_constraint(nama_status, nik_status, dob_status, pob_status)
    if simalarity_value == None:
        simalarity_value == 0.00

    return reccomendation, simalarity_value, nik_status, dob_status, pob_status, outp


# @app.get('/PPATK/', dependencies=[Depends(dttot_get_current_username)])
@app.get('/PPATK/')
async def dttot(Nama, NIK: Optional[str]=None, DOB: Optional[str]=None, POB: Optional[str]=None):
    reccomendation, simalarity_value, nik_status, dob_status, pob_status, outp = dttot_main_funct(Nama, NIK, DOB, POB)
    respond_out = {
        "Recommendation" : reccomendation,
        "Nama Similarity" : simalarity_value,
        "NIK" : nik_status,
        "DOB" : dob_status,
        "POB" : pob_status
    }
    return respond_out


def pep_get_constraint():
    """
    get constraint / schema data for pep
    output : DataFrame
    """
    file_path = "./pep/data/pep_scenario.xlsx"
    df = pd.read_excel(file_path)
    return df

def pep_get_all_data():
    """
    get data
    output : DataFrame
    """
    df = pd.read_csv("./pep/data/Test_all_data_pep.csv")
    df = df.fillna("No Data")
    return df

def pep_DOB_similarity(df, col, dob_input):
    """
    filter DOB column
    output : DataFrame
    """
    df = df[df[col].str.contains(dob_input)].reset_index(drop=True)
    return df

def pep_POB_similarity(df, col, pob_input):
    """
    filter POB column
    output : DataFrame
    """
    try:
        df = df[df[col].str.contains(pob_input)].reset_index(drop=True)
    except:
        df = df[df[col].str.contains(pob_input).fillna(False)].reset_index(drop=True)
    return df

def pep_treatment_constraint(nama_status, dob_status, pob_status):
    df = pep_get_constraint()
    dict_value = {"nama" :nama_status,
                "dob" : dob_status,
                "pob" : pob_status}
    result = df.loc[(df[list(dict_value)] == pd.Series(dict_value)).all(axis=1)]
    result_recommendation =  list(set(result["recommendation"]))[0]
    return result_recommendation


def pep_jaro_distance(s1, s2):
    # lower case all the character
    s1 = s1.lower()
    s2 = s2.lower()

    s1 = s1.strip()
    s2 = s2.strip()

    # If the s are equal
    if (s1 == s2):
        return 1.0

    # Length of two s
    len1 = len(s1)
    len2 = len(s2)

    # Maximum distance upto which matching
    # is allowed
    max_dist = floor(max(len1, len2) / 2) - 1

    # Count of matches
    match = 0

    # Hash for matches
    hash_s1 = [0] * len(s1)
    hash_s2 = [0] * len(s2)

    # Traverse through the first
    for i in range(len1):

        # Check if there is any matches
        for j in range(max(0, i - max_dist),
                       min(len2, i + max_dist + 1)):

            # If there is a match
            if (s1[i] == s2[j] and hash_s2[j] == 0):
                hash_s1[i] = 1
                hash_s2[j] = 1
                match += 1
                break

    # If there is no match
    if (match == 0):
        return 0.0

    # Number of transpositions
    t = 0
    point = 0

    # Count number of occurrences where two characters match but
    # there is a third matched character in between the indices
    for i in range(len1):
        if (hash_s1[i]):

            # Find the next matched character
            # in second
            while (hash_s2[point] == 0):
                point += 1

            if (s1[i] != s2[point]):
                t += 1
            point += 1
    t = t//2

    # Return the Jaro Similarity
    return (match/ len1 + match / len2 +
            (match - t) / match)/ 3.0

def pep_define_list():
    list_gelar_depan = [" kph ", " cn ", " ust ", " drg ", " tgh ", " mayjen tni purn ", " capt "," brigjen tni purn", " h ", " hj ", " kh ", " dr ",
                    " dra ", " drs ", " prof ", " ir ", " jenderal pol  purn ", "  c  ", " hc ", " krt ", " mayjen tni  mar  purn ", " st ", " tb ",
                    " hc ", " drh ", " irjen  pol  purn ", " pdt ", " marsekal tni purn ", " k ", " letnan jenderal tni purn ", " laksdya  tni purn ",
                    " irjen pol purn ", " mayjen tni mar  purn "]

    return list_gelar_depan


def pep_get_url(url):
    headers = {
         'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
     }
    r = requests.get(url, headers=headers)  # Using the custom headers we defined above
    soup = BeautifulSoup(r.content, 'html.parser')
    return soup


def pep_extract_funct(soup, summary):
    for container in soup.findAll('div', {"class":'tF2Cxc'}):
        heading = container.find('h3', {"class" : 'LC20lb MBeuO DKV0Md'}).text
        link = container.find('a')['href']

        summary.append({
          'Heading': heading,
          'Link': link,
        })
    return summary


def pep_get_google(Nama):
    print("query for {} from google...".format(Nama))
    # query for 1st page
    url = "https://www.google.co.id/search?q={}".format(Nama)
    soup = pep_get_url(url)
    summary = []
    res = pep_extract_funct(soup, summary)

    # query for 2nd page
    try:
        base_url = "https://www.google.co.id"
        page_2 = soup.find('a', {"aria-label":'Page 2'})['href']
        url2 = base_url+page_2
        soup2 = pep_get_url(url)
        res = pep_extract_funct(soup2, res)
    except:
        pass
    return res[:10]


@app.get('/PEP/')
async def dprd_tk1(Nama, DOB: Optional[str]=None, POB: Optional[str]=None):
    query = Nama
    nama_status = "not match"
    dob_status = "not match"
    pob_status = "not match"

    list_gelar_depan = pep_define_list()
    regex = re.compile('[^a-zA-Z]')

    # Nama preprcessing
    Nama = Nama.lower()
    Nama = Nama.replace(",", " ")
    Nama = Nama.replace(".", " ")
    Nama = " " + Nama
    filter_str = '|'.join(list_gelar_depan)
    for x in range(5):
        Nama = re.sub(filter_str, ' ', Nama)
    Nama_prepro = regex.sub('', Nama)

    df = pep_get_all_data()
    df["score"] = df['only_character'].apply(lambda x: pep_jaro_distance(x, Nama_prepro))
    df = df.sort_values(by="score", ascending=False).reset_index(drop=True)

    # filter nama
    df_nama = df[df["score"] >= 0.75].reset_index(drop=True)
    if df_nama.shape[0] > 0:
        nama_status = "match"
        if DOB is not None:
            DOB = DOB.strip()
            DOB = DOB.lower()
            df_DOB = pep_DOB_similarity(df_nama, 'tanggal lahir', DOB)
            if df_DOB.shape[0] > 0:
                df_nama = df_DOB
                dob_status = "match"

        if POB is not None:
            POB = POB.strip()
            POB = POB.lower()
            df_POB = pep_POB_similarity(df_nama, 'tempat lahir', POB)
            if df_POB.shape[0] > 0:
                df_nama = df_POB
                pob_status = "match"
        df_show = df_nama.copy()
        df_show = df_show.head(10)
    else:
        df_show = df_nama.copy()
        df_show = df_show.head(10)
    reccomendation = pep_treatment_constraint(nama_status, dob_status, pob_status)


    if df_show.shape[0] > 0:
        query = df['Remove Gelar Depan'][0]
    else:
        pass


    if reccomendation == "Phase 2" or reccomendation == "PEP":
        # list_idx, top_ten = news_filter(df_show["Nama"][0])
        try:
            top_ten = pep_get_google(query)
        except:
            top_ten = []
    else:
        # list_idx = []
        top_ten = []

    cols = ["Nama", "tempat lahir", "tanggal lahir", "score"]
    df_show = df_show[cols]

    if df_show.shape[0] > 0:
        nama_similarity_score = df_show["score"][0]
    else:
        nama_similarity_score = 0.00

    respond_out = {
        "Recommendation" : reccomendation,
        "Nama Similarity" : nama_similarity_score,
        "DOB" : dob_status,
        "POB" : pob_status,
        # "User_Input" : Nama_prepro,
        # "Output" : df_show,
        # "Filtered_News" : list_idx,
        "Top_10_Google_Search" : top_ten
    }
    return respond_out


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8090, log_level="info", reload=True)

# to run python api.py
# go here http://127.0.0.1:8090/docs
