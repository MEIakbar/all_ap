from bs4 import BeautifulSoup
import requests
from urllib.request import urlopen, urlretrieve, quote
from urllib.parse import urljoin
import pandas as pd
from tqdm import tqdm
import csv
import json
import os 


class get_dpr_tk2():

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
            "dprd_url1": auth_dict["Aceh"]["Kabupaten Aceh Barat"],
            "dprd_url2": auth_dict["Aceh"]["Kabupaten Aceh Barat Daya"],
            "dprd_url3": auth_dict["Aceh"]["Kabupaten Aceh Besar"],
            "dprd_url4": auth_dict["Aceh"]["Kabupaten Aceh Jaya"],
            "dprd_url5": auth_dict["Aceh"]["Kabupaten Aceh Selatan"],
            "dprd_url6": auth_dict["Aceh"]["Kabupaten Aceh Singkil"],
            "dprd_url7": auth_dict["Aceh"]["Kabupaten Aceh Tamiang"],
            "dprd_url8": auth_dict["Aceh"]["Kabupaten Aceh Tengah"],
            "dprd_url9": auth_dict["Aceh"]["Kabupaten Aceh Tenggara"],
            "dprd_url10": auth_dict["Aceh"]["Kabupaten Aceh Timur"],
            "dprd_url11": auth_dict["Aceh"]["Kabupaten Aceh Utara"],
            "dprd_url12": auth_dict["Aceh"]["Kabupaten Bener Meriah"],
            "dprd_url13": auth_dict["Aceh"]["Kabupaten Bireuen"],
            "dprd_url14": auth_dict["Aceh"]["Kabupaten Gayo Lues"],
            "dprd_url15": auth_dict["Aceh"]["Kabupaten Nagan Raya"],
            "dprd_url16": auth_dict["Aceh"]["Kabupaten Pidie"],
            "dprd_url17": auth_dict["Aceh"]["Kabupaten Pidie Jaya"],
            "dprd_url18": auth_dict["Aceh"]["Kabupaten Simeulue"],
            "dprd_url19": auth_dict["Aceh"]["Kota Banda Aceh"],
            "dprd_url20": auth_dict["Aceh"]["Kota Langsa"],
            "dprd_url21": auth_dict["Aceh"]["Kota Lhokseumawe"],
            "dprd_url22": auth_dict["Aceh"]["Kota Sabang"],
            "dprd_url23": auth_dict["Aceh"]["Kota Subulussalam"],
            "dprd_url24": auth_dict["Sumatera Utara"]["Kabupaten Asahan1"],
            "dprd_url25": auth_dict["Sumatera Utara"]["Kabupaten Asahan2"],
            "dprd_url26": auth_dict["Sumatera Utara"]["Kabupaten Asahan3"],
            "dprd_url27": auth_dict["Sumatera Utara"]["Kabupaten Batubara1"],
            "dprd_url28": auth_dict["Sumatera Utara"]["Kabupaten Batubara2"],
            "dprd_url29": auth_dict["Sumatera Utara"]["Kabupaten Batubara3"],
            "dprd_url30": auth_dict["Sumatera Utara"]["Kabupaten Batubara4"],
            "dprd_url31": auth_dict["Sumatera Utara"]["Kabupaten Batubara5"],
            "dprd_url32": auth_dict["Sumatera Utara"]["Kabupaten Batubara6"],
            "dprd_url33": auth_dict["Sumatera Utara"]["Kabupaten Batubara7"],
            "dprd_url34": auth_dict["Sumatera Utara"]["Kabupaten Batubara8"],
            "dprd_url35": auth_dict["Sumatera Utara"]["Kabupaten Batubara9"],
            "dprd_url36": auth_dict["Sumatera Utara"]["Kabupaten Batubara10"],
            "dprd_url37": auth_dict["Sumatera Utara"]["Kabupaten Dairi"],
            "dprd_url38": auth_dict["Sumatera Utara"]["Kabupaten Deli Serdang"],
            "dprd_url39": auth_dict["Sumatera Utara"]["Kabupaten Humbang Hasundutan"],
            "dprd_url40": auth_dict["Sumatera Utara"]["Kabupaten Karo"],
            "dprd_url41": auth_dict["Sumatera Utara"]["Kabupaten Labuhanbatu"],
            "dprd_url42": auth_dict["Sumatera Utara"]["Kabupaten Labuhanbatu Selatan"],
            "dprd_url43": auth_dict["Sumatera Utara"]["Kabupaten Labuhanbatu Utara"],
            "dprd_url44": auth_dict["Sumatera Utara"]["Kabupaten Langkat"],
            "dprd_url45": auth_dict["Sumatera Utara"]["Kabupaten Mandailing Natal"],
            "dprd_url46": auth_dict["Sumatera Utara"]["Kabupaten Nias"],
            "dprd_url47": auth_dict["Sumatera Utara"]["Kabupaten Nias Barat"],
            "dprd_url48": auth_dict["Sumatera Utara"]["Kabupaten Nias Selatan"],
            "dprd_url49": auth_dict["Sumatera Utara"]["Kabupaten Nias Utara"],
            "dprd_url50": auth_dict["Sumatera Utara"]["Kabupaten Padang Lawas"],
            "dprd_url51": auth_dict["Sumatera Utara"]["Kabupaten Padang Lawas Utara"],
            "dprd_url52": auth_dict["Sumatera Utara"]["Kabupaten Pakpak Bharat"],
            "dprd_url53": auth_dict["Sumatera Utara"]["Kabupaten Samosir"],
            "dprd_url54": auth_dict["Sumatera Utara"]["Kabupaten Serdang Bedagai"],
            "dprd_url55": auth_dict["Sumatera Utara"]["Kabupaten Simalungun"],
            "dprd_url56": auth_dict["Sumatera Utara"]["Kabupaten Tapanuli Selatan"],
            "dprd_url57": auth_dict["Sumatera Utara"]["Kabupaten Tapanuli Tengah"],
            "dprd_url58": auth_dict["Sumatera Utara"]["Kabupaten Tapanuli Utara"],
            "dprd_url59": auth_dict["Sumatera Utara"]["Kabupaten Toba Samosir"],
            "dprd_url60": auth_dict["Sumatera Utara"]["Kota Binjai"],
            "dprd_url61": auth_dict["Sumatera Utara"]["Kota Gunungsitoli"],
            "dprd_url62": auth_dict["Sumatera Utara"]["Kota Medan"],
            "dprd_url63": auth_dict["Sumatera Utara"]["Kota Padangsidempuan"],
            "dprd_url64": auth_dict["Sumatera Utara"]["Kota Pematangsiantar"],
            "dprd_url65": auth_dict["Sumatera Utara"]["Kota Sibolga"],
            "dprd_url66": auth_dict["Sumatera Utara"]["Kota Tanjungbalai"],
            "dprd_url67": auth_dict["Sumatera Utara"]["Kota Tebing Tinggi"],
            "dprd_url68": auth_dict["Sumatera Barat"]["Kabupaten Agam"],
            "dprd_url69": auth_dict["Sumatera Barat"]["Kabupaten Dharmasraya"],
            "dprd_url70": auth_dict["Sumatera Barat"]["Kabupaten Kepulauan Mentawai"],
            "dprd_url71": auth_dict["Sumatera Barat"]["Kabupaten Lima Puluh Kota"],
            "dprd_url72": auth_dict["Sumatera Barat"]["Kabupaten Padang Pariaman"],
            "dprd_url73": auth_dict["Sumatera Barat"]["Kabupaten Pasaman"],
            "dprd_url74": auth_dict["Sumatera Barat"]["Kabupaten Pasaman Barat"],
            "dprd_url75": auth_dict["Sumatera Barat"]["Kabupaten Pesisir Selatan"],
            "dprd_url76": auth_dict["Sumatera Barat"]["Kabupaten Sijunjung"],
            "dprd_url77": auth_dict["Sumatera Barat"]["Kabupaten Solok"],
            "dprd_url78": auth_dict["Sumatera Barat"]["Kabupaten Solok Selatan"],
            "dprd_url79": auth_dict["Sumatera Barat"]["Kabupaten Tanah Datar"],
            "dprd_url80": auth_dict["Sumatera Barat"]["Kota Bukittinggi"],
            "dprd_url81": auth_dict["Sumatera Barat"]["Kota Padang"],
            "dprd_url82": auth_dict["Sumatera Barat"]["Kota Padangpanjang"],
            "dprd_url83": auth_dict["Sumatera Barat"]["Kota Pariaman"],
            "dprd_url84": auth_dict["Sumatera Barat"]["Kota Payakumbuh"],
            "dprd_url85": auth_dict["Sumatera Barat"]["Kota Sawahlunto"],
            "dprd_url86": auth_dict["Sumatera Barat"]["Kota Solok"],
            "dprd_url87": auth_dict["Sumatera Selatan"]["Kabupaten Banyuasin"],
            "dprd_url88": auth_dict["Sumatera Selatan"]["Kabupaten Empat Lawang"],
            "dprd_url89": auth_dict["Sumatera Selatan"]["Kabupaten Lahat"],
            "dprd_url90": auth_dict["Sumatera Selatan"]["Kabupaten Muara Enim"],
            "dprd_url91": auth_dict["Sumatera Selatan"]["Kabupaten Musi Banyuasin"],
            "dprd_url92": auth_dict["Sumatera Selatan"]["Kabupaten Musi Rawas"],
            "dprd_url93": auth_dict["Sumatera Selatan"]["Kabupaten Musi Rawas Utara"],
            "dprd_url94": auth_dict["Sumatera Selatan"]["Kabupaten Ogan Ilir"],
            "dprd_url95": auth_dict["Sumatera Selatan"]["Kabupaten Ogan Komering Ilir"],
            "dprd_url96": auth_dict["Sumatera Selatan"]["Kabupaten Ogan Komering Ulu"],
            "dprd_url97": auth_dict["Sumatera Selatan"]["Kabupaten Ogan Komering Ulu Selatan"], 
            "dprd_url98": auth_dict["Sumatera Selatan"]["Kabupaten Ogan Komering Ulu Timur"],
            "dprd_url99": auth_dict["Sumatera Selatan"]["Kabupaten Penukal Abab Lematang Ilir"], 
            "dprd_url100": auth_dict["Sumatera Selatan"]["Kota Lubuklinggau"],
            "dprd_url101": auth_dict["Sumatera Selatan"]["Kota Pagar Alam"],
            "dprd_url102": auth_dict["Sumatera Selatan"]["Kota Palembang"],
            "dprd_url103": auth_dict["Sumatera Selatan"]["Kota Prabumulih1"],
            "dprd_url104": auth_dict["Sumatera Selatan"]["Kota Prabumulih2"],
            "dprd_url105": auth_dict["Sumatera Selatan"]["Kota Prabumulih3"],
            "dprd_url106": auth_dict["Provinsi Riau"]["Kabupaten Bengkalis"],
            "dprd_url107": auth_dict["Provinsi Riau"]["Kabupaten Indragiri Hilir"],
            "dprd_url108": auth_dict["Provinsi Riau"]["Kabupaten Indragiri Huluh"],
            "dprd_url109": auth_dict["Provinsi Riau"]["Kabupaten Kampar"],
            "dprd_url110": auth_dict["Provinsi Riau"]["Kabupaten Kepulauan Meranti"],
            "dprd_url111": auth_dict["Provinsi Riau"]["Kabupaten Kuantan Singingi"],
            "dprd_url112": auth_dict["Provinsi Riau"]["Kabupaten Pelalawan"],
            "dprd_url113": auth_dict["Provinsi Riau"]["Kabupaten Rokan Hilir"],
            "dprd_url114": auth_dict["Provinsi Riau"]["Kabupaten Rokan Hulu"],
            "dprd_url115": auth_dict["Provinsi Riau"]["Kabupaten Siak"],
            "dprd_url116": auth_dict["Provinsi Riau"]["Kota Dumai"],
            "dprd_url117": auth_dict["Provinsi Riau"]["Kota Pekanbaru"],
            "dprd_url118": auth_dict["Kepulauan Riau"]["Kabupaten Bintan"],
            "dprd_url119": auth_dict["Kepulauan Riau"]["Kabupaten Karimun"],
            "dprd_url120": auth_dict["Kepulauan Riau"]["Kabupaten Kepulauan Anambas"],
            "dprd_url121": auth_dict["Kepulauan Riau"]["Kabupaten Lingga"],
            "dprd_url122": auth_dict["Kepulauan Riau"]["Kabupaten Natuna"],
            "dprd_url123": auth_dict["Kepulauan Riau"]["Kota Batam"],
            "dprd_url124": auth_dict["Kepulauan Riau"]["Kota Tanjung Pinang"],
            "dprd_url125": auth_dict["Provinsi Jambi"]["Kabupaten Batanghari"],
            "dprd_url126": auth_dict["Provinsi Jambi"]["Kabupaten Bungo"],
            "dprd_url127": auth_dict["Provinsi Jambi"]["Kabupaten Kerinci"],
            "dprd_url128": auth_dict["Provinsi Jambi"]["Kabupaten Merangin"],
            "dprd_url129": auth_dict["Provinsi Jambi"]["Kabupaten Muaro Jambi"],
            "dprd_url130": auth_dict["Provinsi Jambi"]["Kabupaten Sarolangun"],
            "dprd_url131": auth_dict["Provinsi Jambi"]["Kabupaten Tanjung Jabung Barat"],
            "dprd_url132": auth_dict["Provinsi Jambi"]["Kabupaten Tanjung Jabung Timur"],
            "dprd_url133": auth_dict["Provinsi Jambi"]["Kabupaten Tebo"],
            "dprd_url134": auth_dict["Provinsi Jambi"]["Kota Jambi"],
            "dprd_url135": auth_dict["Provinsi Jambi"]["Kota Sungai Penuh"],
            "dprd_url136": auth_dict["Bengkulu"]["Kabupaten Bengkulu Selatan"],
            "dprd_url137": auth_dict["Bengkulu"]["Kabupaten Bengkulu Tengah"],
            "dprd_url138": auth_dict["Bengkulu"]["Kabupaten Bengkulu Utara"],
            "dprd_url139": auth_dict["Bengkulu"]["Kabupaten Kaur"],
            "dprd_url140": auth_dict["Bengkulu"]["Kabupaten Kepahiang"],
            "dprd_url141": auth_dict["Bengkulu"]["Kabupaten Lebong"],
            "dprd_url142": auth_dict["Bengkulu"]["Kabupaten Mukomuko"],
            "dprd_url143": auth_dict["Bengkulu"]["Kabupaten Rejang Lebong"],
            "dprd_url144": auth_dict["Bengkulu"]["Kabupaten Seluma"],
            "dprd_url145": auth_dict["Bengkulu"]["Kota Bengkulu"],
            "dprd_url146": auth_dict["Bangka Belitung"]["Kabupaten Bangka"],
            "dprd_url147": auth_dict["Bangka Belitung"]["Kabupaten Bangka Barat"],
            "dprd_url148": auth_dict["Bangka Belitung"]["Kabupaten Bangka Selatan"],
            "dprd_url149": auth_dict["Bangka Belitung"]["Kabupaten Bangka Tengah"],
            "dprd_url150": auth_dict["Bangka Belitung"]["Kabupaten Belitung"],
            "dprd_url151": auth_dict["Bangka Belitung"]["Kabupaten Belitung Timur"],
            "dprd_url152": auth_dict["Bangka Belitung"]["Kota Pangkal Pinang"],
            "dprd_url153_base": auth_dict["Lampung"]["Kabupaten Lampung Tengah 1"],
            "dprd_url153_1": auth_dict["Lampung"]["Kabupaten Lampung Tengah 2"],
            "dprd_url153_2": auth_dict["Lampung"]["Kabupaten Lampung Tengah 3"],
            "dprd_url153_3": auth_dict["Lampung"]["Kabupaten Lampung Tengah 4"],
            "dprd_url153_4": auth_dict["Lampung"]["Kabupaten Lampung Tengah 5"],
            "dprd_url153_5": auth_dict["Lampung"]["Kabupaten Lampung Tengah 6"],
            "dprd_url153_6": auth_dict["Lampung"]["Kabupaten Lampung Tengah 7"],
            "dprd_url154": auth_dict["Lampung"]["Kabupaten Lampung Utara"],
            "dprd_url155": auth_dict["Lampung"]["Kabupaten Lampung Selatan"],
            "dprd_url156_base": auth_dict["Lampung"]["Kabupaten Lampung Barat 1"],
            "dprd_url156_1": auth_dict["Lampung"]["Kabupaten Lampung Barat 2"],
            "dprd_url156_2": auth_dict["Lampung"]["Kabupaten Lampung Barat 3"],
            "dprd_url156_3": auth_dict["Lampung"]["Kabupaten Lampung Barat 4"],
            "dprd_url156_4": auth_dict["Lampung"]["Kabupaten Lampung Barat 5"],
            "dprd_url156_5": auth_dict["Lampung"]["Kabupaten Lampung Barat 6"],
            "dprd_url157": auth_dict["Lampung"]["Kabupaten Lampung Timur"],
            "dprd_url158": auth_dict["Lampung"]["Kabupaten Mesuji"],
            "dprd_url159": auth_dict["Lampung"]["Kabupaten Pesawaran"],
            "dprd_url160": auth_dict["Lampung"]["Kabupaten Pesisir Barat"],
            "dprd_url161": auth_dict["Lampung"]["Kabupaten Pringsewu"],
            "dprd_url162": auth_dict["Lampung"]["Kabupaten Tulang Bawang"],
            "dprd_url163": auth_dict["Lampung"]["Kabupaten Tulang Bawang Barat"],
            "dprd_url164": auth_dict["Lampung"]["Kabupaten Tanggamus"],
            "dprd_url165": auth_dict["Lampung"]["Kabupaten Way Kanan"],
            "dprd_url166": auth_dict["Lampung"]["Kota Bandar Lampung"],
            "dprd_url167": auth_dict["Lampung"]["Kota Metro"],
            "dprd_url168": auth_dict["Banten"]["Kabupaten Lebak"],
            "dprd_url169": auth_dict["Banten"]["Kabupaten Pandeglang"],
            "dprd_url170": auth_dict["Banten"]["Kabupaten Serangall"],
            "dprd_url171": auth_dict["Banten"]["Kabupaten Tangerang"],
            "dprd_url172": auth_dict["Banten"]["Kota Cilegon"],
            "dprd_url173_1": auth_dict["Banten"]["Kota Serang 1"],
            "dprd_url173_2": auth_dict["Banten"]["Kota Serang 2"],
            "dprd_url173_3": auth_dict["Banten"]["Kota Serang 3"],
            "dprd_url173_4": auth_dict["Banten"]["Kota Serang 4"],
            "dprd_url173_5": auth_dict["Banten"]["Kota Serang 5"],
            "dprd_url174": auth_dict["Banten"]["Kota Tangerang"],
            "dprd_url175": auth_dict["Banten"]["Kota Tangerang Selatan1"],
            "dprd_url176": auth_dict["Banten"]["Kota Tangerang Selatan2"],
            "dprd_url177": auth_dict["Banten"]["Kota Tangerang Selatan3"],
            "dprd_url178": auth_dict["Banten"]["Kota Tangerang Selatan4"],
            "dprd_url179": auth_dict["Banten"]["Kota Tangerang Selatan5"],
            "dprd_url180": auth_dict["Banten"]["Kota Tangerang Selatan6"],
            "dprd_url181": auth_dict["Banten"]["Kota Tangerang Selatan7"],
            "dprd_url182": auth_dict["Jawa Barat"]["Kabupaten Bandung"],
            "dprd_url183": auth_dict["Jawa Barat"]["Kabupaten Bandung Barat"],
            "dprd_url184": auth_dict["Jawa Barat"]["Kabupaten Bekasi1"],
            "dprd_url185": auth_dict["Jawa Barat"]["Kabupaten Bekasi2"],
            "dprd_url186": auth_dict["Jawa Barat"]["Kabupaten Bekasi3"],
            "dprd_url187": auth_dict["Jawa Barat"]["Kabupaten Bekasi4"],
            "dprd_url188": auth_dict["Jawa Barat"]["Kabupaten Bekasi5"],
            "dprd_url189": auth_dict["Jawa Barat"]["Kabupaten Bekasi6"],
            "dprd_url190": auth_dict["Jawa Barat"]["Kabupaten Bogor"],
            "dprd_url191": auth_dict["Jawa Barat"]["Kabupaten Ciamis"],
            "dprd_url192": auth_dict["Jawa Barat"]["Kabupaten Cianjur"],
            "dprd_url193": auth_dict["Jawa Barat"]["Kabupaten Cirebon"],
            "dprd_url194": auth_dict["Jawa Barat"]["Kabupaten Garut"],
            "dprd_url195": auth_dict["Jawa Barat"]["Kabupaten Indramayu"],
            "dprd_url196": auth_dict["Jawa Barat"]["Kabupaten Karawang"],
            "dprd_url197": auth_dict["Jawa Barat"]["Kabupaten Kuningan"],
            "dprd_url198": auth_dict["Jawa Barat"]["Kabupaten Majalengka"],
            "dprd_url199": auth_dict["Jawa Barat"]["Kabupaten Pangandaran"],
            "dprd_url200": auth_dict["Jawa Barat"]["Kabupaten Purwakarta"],
            "dprd_url201": auth_dict["Jawa Barat"]["Kabupaten Subang"],
            "dprd_url202": auth_dict["Jawa Barat"]["Kabupaten Sukabumi"],
            "dprd_url203": auth_dict["Jawa Barat"]["Kabupaten Sumedang"],
            "dprd_url204": auth_dict["Jawa Barat"]["Kabupaten Tasikmalaya"],
            "dprd_url204_1": auth_dict["Jawa Barat"]["Kabupaten Tasikmalaya 1"],
            "dprd_url204_2": auth_dict["Jawa Barat"]["Kabupaten Tasikmalaya 2"],
            "dprd_url204_3": auth_dict["Jawa Barat"]["Kabupaten Tasikmalaya 3"],
            "dprd_url204_4": auth_dict["Jawa Barat"]["Kabupaten Tasikmalaya 4"],
            "dprd_url204_5": auth_dict["Jawa Barat"]["Kabupaten Tasikmalaya 5"],
            "dprd_url205": auth_dict["Jawa Barat"]["Kota Bandung"],
            "dprd_url206": auth_dict["Jawa Barat"]["Kota Banjar"],
            "dprd_url207": auth_dict["Jawa Barat"]["Kota Bekasi"],
            "dprd_url208": auth_dict["Jawa Barat"]["Kota Bogor"],
            "dprd_url209": auth_dict["Jawa Barat"]["Kota Cimahi"],
            "dprd_url210": auth_dict["Jawa Barat"]["Kota Cirebon"],
            "dprd_url211": auth_dict["Jawa Barat"]["Kota Depok1"],
            "dprd_url212": auth_dict["Jawa Barat"]["Kota Depok2"],
            "dprd_url213": auth_dict["Jawa Barat"]["Kota Depok3"],
            "dprd_url214": auth_dict["Jawa Barat"]["Kota Depok4"],
            "dprd_url215": auth_dict["Jawa Barat"]["Kota Depok5"],
            "dprd_url216": auth_dict["Jawa Barat"]["Kota Depok6"],
            "dprd_url217": auth_dict["Jawa Barat"]["Kota Depok7"],
            "dprd_url218": auth_dict["Jawa Barat"]["Kota Sukabumi"],
            "dprd_url219": auth_dict["Jawa Barat"]["Kota Tasikmalaya"],
            "dprd_url220": auth_dict["Jawa Tengah"]["Kabupaten Banjarnegara"],
            "dprd_url221": auth_dict["Jawa Tengah"]["Kabupaten Banyumas"],
            "dprd_url222": auth_dict["Jawa Tengah"]["Kabupaten Batang"],
            "dprd_url223": auth_dict["Jawa Tengah"]["Kabupaten Blora"],
            "dprd_url224": auth_dict["Jawa Tengah"]["Kabupaten Boyolali"],
            "dprd_url225": auth_dict["Jawa Tengah"]["Kabupaten Brebes"],
            "dprd_url226": auth_dict["Jawa Tengah"]["Kabupaten Cilacapall"],
            "dprd_url227": auth_dict["Jawa Tengah"]["Kabupaten Demak"],
            "dprd_url228": auth_dict["Jawa Tengah"]["Kabupaten Grobogan"],
            "dprd_url229": auth_dict["Jawa Tengah"]["Kabupaten Jepara"],
            "dprd_url230": auth_dict["Jawa Tengah"]["Kabupaten Karanganyar"],
            "dprd_url231": auth_dict["Jawa Tengah"]["Kabupaten Kebumen"],
            "dprd_url232": auth_dict["Jawa Tengah"]["Kabupaten Kendal"],
            "dprd_url233": auth_dict["Jawa Tengah"]["Kabupaten Klaten"],
            "dprd_url234": auth_dict["Jawa Tengah"]["Kabupaten Kudus"],
            "dprd_url235": auth_dict["Jawa Tengah"]["Kabupaten Magelang"],
            "dprd_url236": auth_dict["Jawa Tengah"]["Kabupaten Pati"],
            "dprd_url237_1": auth_dict["Jawa Tengah"]["Kabupaten Pekalongan 1"],
            "dprd_url237_2": auth_dict["Jawa Tengah"]["Kabupaten Pekalongan 2"],
            "dprd_url237_3": auth_dict["Jawa Tengah"]["Kabupaten Pekalongan 3"],
            "dprd_url237_4": auth_dict["Jawa Tengah"]["Kabupaten Pekalongan 4"],
            "dprd_url238": auth_dict["Jawa Tengah"]["Kabupaten Pemalang"],
            "dprd_url239": auth_dict["Jawa Tengah"]["Kabupaten Purbalingga"],
            "dprd_url240": auth_dict["Jawa Tengah"]["Kabupaten Purworejo"],
            "dprd_url241": auth_dict["Jawa Tengah"]["Kabupaten Rembang"],
            "dprd_url242": auth_dict["Jawa Tengah"]["Kabupaten Semarang"],
            "dprd_url243": auth_dict["Jawa Tengah"]["Kabupaten Sragen"],
            "dprd_url244": auth_dict["Jawa Tengah"]["Kabupaten Sukoharjo"],
            "dprd_url245": auth_dict["Jawa Tengah"]["Kabupaten Tegal1"],
            "dprd_url246": auth_dict["Jawa Tengah"]["Kabupaten Tegal2"],
            "dprd_url247": auth_dict["Jawa Tengah"]["Kabupaten Tegal3"],
            "dprd_url248": auth_dict["Jawa Tengah"]["Kabupaten Tegal4"],
            "dprd_url249": auth_dict["Jawa Tengah"]["Kabupaten Tegal5"],
            "dprd_url250": auth_dict["Jawa Tengah"]["Kabupaten Tegal6"],
            "dprd_url251": auth_dict["Jawa Tengah"]["Kabupaten Tegal7"],
            "dprd_url252": auth_dict["Jawa Tengah"]["Kabupaten Tegal8"],
            "dprd_url253": auth_dict["Jawa Tengah"]["Kabupaten Tegal9"],
            "dprd_url254": auth_dict["Jawa Tengah"]["Kabupaten Tegal10"],
            "dprd_url255": auth_dict["Jawa Tengah"]["Kabupaten Tegal11"],
            "dprd_url256": auth_dict["Jawa Tengah"]["Kabupaten Tegal12"],
            "dprd_url257": auth_dict["Jawa Tengah"]["Kabupaten Tegal13"],
            "dprd_url258": auth_dict["Jawa Tengah"]["Kabupaten Tegal14"],   
            "dprd_url259": auth_dict["Jawa Tengah"]["Kabupaten Temanggung"],
            "dprd_url260_1": auth_dict["Jawa Tengah"]["Kabupaten Wonogiri 1"],
            "dprd_url260_2": auth_dict["Jawa Tengah"]["Kabupaten Wonogiri 2"],
            "dprd_url260_3": auth_dict["Jawa Tengah"]["Kabupaten Wonogiri 3"],
            "dprd_url260_4": auth_dict["Jawa Tengah"]["Kabupaten Wonogiri 4"],
            "dprd_url260_5": auth_dict["Jawa Tengah"]["Kabupaten Wonogiri 5"],
            "dprd_url261": auth_dict["Jawa Tengah"]["Kabupaten Wonosobo"],
            "dprd_url262": auth_dict["Jawa Tengah"]["Kota Magelang"],
            "dprd_url263": auth_dict["Jawa Tengah"]["Kota Pekalongan"],
            "dprd_url264": auth_dict["Jawa Tengah"]["Kota Salatiga"],
            "dprd_url265": auth_dict["Jawa Tengah"]["Kota Semarang"],
            "dprd_url265": auth_dict["Jawa Tengah"]["Kota Semarang"],
            "dprd_url265_1": auth_dict["Jawa Tengah"]["Kota Semarang 1"],
            "dprd_url265_2": auth_dict["Jawa Tengah"]["Kota Semarang 2"],
            "dprd_url265_3": auth_dict["Jawa Tengah"]["Kota Semarang 3"],
            "dprd_url265_4": auth_dict["Jawa Tengah"]["Kota Semarang 4"],
            "dprd_url266": auth_dict["Jawa Tengah"]["Kota Surakarta1"],
            "dprd_url267": auth_dict["Jawa Tengah"]["Kota Surakarta2"],
            "dprd_url268": auth_dict["Jawa Tengah"]["Kota Surakarta3"],
            "dprd_url269": auth_dict["Jawa Tengah"]["Kota Surakarta4"],
            "dprd_url270": auth_dict["Jawa Tengah"]["Kota Tegal"],
            "dprd_url271": auth_dict["Jawa Timur"]["Kabupaten Bangkalan1"],
            "dprd_url272": auth_dict["Jawa Timur"]["Kabupaten Bangkalan2"],
            "dprd_url273": auth_dict["Jawa Timur"]["Kabupaten Bangkalan3"],
            "dprd_url274": auth_dict["Jawa Timur"]["Kabupaten Bangkalan4"],
            "dprd_url275": auth_dict["Jawa Timur"]["Kabupaten Bangkalan5"],
            "dprd_url276": auth_dict["Jawa Timur"]["Kabupaten Banyuwangi"],
            "dprd_url277": auth_dict["Jawa Timur"]["Kabupaten Blitar"],
            "dprd_url278": auth_dict["Jawa Timur"]["Kabupaten Bojonegoro1"],
            "dprd_url279": auth_dict["Jawa Timur"]["Kabupaten Bojonegoro2"],
            "dprd_url280": auth_dict["Jawa Timur"]["Kabupaten Bojonegoro3"],
            "dprd_url281": auth_dict["Jawa Timur"]["Kabupaten Bojonegoro4"],
            "dprd_url282": auth_dict["Jawa Timur"]["Kabupaten Bondowoso1"],
            "dprd_url283": auth_dict["Jawa Timur"]["Kabupaten Bondowoso2"],
            "dprd_url284": auth_dict["Jawa Timur"]["Kabupaten Bondowoso3"],
            "dprd_url285": auth_dict["Jawa Timur"]["Kabupaten Bondowoso4"],
            "dprd_url286": auth_dict["Jawa Timur"]["Kabupaten Bondowoso5"],
            "dprd_url287": auth_dict["Jawa Timur"]["Kabupaten Bondowoso6"],
            "dprd_url288_0": auth_dict["Jawa Timur"]["Kabupaten Gresik0"],
            "dprd_url288": auth_dict["Jawa Timur"]["Kabupaten Gresik1"],
            "dprd_url289": auth_dict["Jawa Timur"]["Kabupaten Gresik2"],
            "dprd_url290": auth_dict["Jawa Timur"]["Kabupaten Gresik3"],
            "dprd_url291": auth_dict["Jawa Timur"]["Kabupaten Gresik4"],
            "dprd_url292": auth_dict["Jawa Timur"]["Kabupaten Gresik5"],
            "dprd_url293": auth_dict["Jawa Timur"]["Kabupaten Gresik6"],
            "dprd_url294": auth_dict["Jawa Timur"]["Kabupaten Gresik7"],
            "dprd_url295": auth_dict["Jawa Timur"]["Kabupaten Jember"],
            "dprd_url296": auth_dict["Jawa Timur"]["Kabupaten Jombang"],
            "dprd_url297": auth_dict["Jawa Timur"]["Kabupaten Kediri"],
            "dprd_url298": auth_dict["Jawa Timur"]["Kabupaten Lamongan"],
            "dprd_url299": auth_dict["Jawa Timur"]["Kabupaten Lumajang1"],
            "dprd_url300_1": auth_dict["Jawa Timur"]["Kabupaten Lumajang2"],
            "dprd_url300_2": auth_dict["Jawa Timur"]["Kabupaten Lumajang3"],
            "dprd_url300_3": auth_dict["Jawa Timur"]["Kabupaten Lumajang4"],
            "dprd_url300_4": auth_dict["Jawa Timur"]["Kabupaten Lumajang5"],
            "dprd_url301": auth_dict["Jawa Timur"]["Kabupaten Madiun1"],
            "dprd_url302": auth_dict["Jawa Timur"]["Kabupaten Madiun2"],
            "dprd_url303": auth_dict["Jawa Timur"]["Kabupaten Madiun3"],
            "dprd_url304": auth_dict["Jawa Timur"]["Kabupaten Magetan"],
            "dprd_url305": auth_dict["Jawa Timur"]["Kabupaten Malang"],
            "dprd_url306": auth_dict["Jawa Timur"]["Kabupaten Mojokerto"],
            "dprd_url307": auth_dict["Jawa Timur"]["Kabupaten Nganjuk"],
            "dprd_url308": auth_dict["Jawa Timur"]["Kabupaten Ngawi"],
            "dprd_url309": auth_dict["Jawa Timur"]["Kabupaten Pacitan1"],
            "dprd_url310": auth_dict["Jawa Timur"]["Kabupaten Pacitan2"],
            "dprd_url311": auth_dict["Jawa Timur"]["Kabupaten Pacitan3"],
            "dprd_url312": auth_dict["Jawa Timur"]["Kabupaten Pamekasan"],
            "dprd_url313": auth_dict["Jawa Timur"]["Kabupaten Pasuruan1"],
            "dprd_url314": auth_dict["Jawa Timur"]["Kabupaten Pasuruan2"],
            "dprd_url315": auth_dict["Jawa Timur"]["Kabupaten Ponorogo"],
            "dprd_url316": auth_dict["Jawa Timur"]["Kabupaten Probolinggo1"],
            "dprd_url317": auth_dict["Jawa Timur"]["Kabupaten Probolinggo2"],
            "dprd_url318": auth_dict["Jawa Timur"]["Kabupaten Probolinggo3"],
            "dprd_url319": auth_dict["Jawa Timur"]["Kabupaten Probolinggo4"],
            "dprd_url320": auth_dict["Jawa Timur"]["Kabupaten Probolinggo5"],
            "dprd_url321": auth_dict["Jawa Timur"]["Kabupaten Probolinggo6"],
            "dprd_url322": auth_dict["Jawa Timur"]["Kabupaten Probolinggo7"],
            "dprd_url323": auth_dict["Jawa Timur"]["Kabupaten Sampang"],
            "dprd_url324": auth_dict["Jawa Timur"]["Kabupaten Sidoarjo"],
            "dprd_url325": auth_dict["Jawa Timur"]["Kabupaten Situbondo"],
            "dprd_url326": auth_dict["Jawa Timur"]["Kabupaten Sumenep1"],
            "dprd_url327": auth_dict["Jawa Timur"]["Kabupaten Sumenep2"],
            "dprd_url328": auth_dict["Jawa Timur"]["Kabupaten Sumenep3"],
            "dprd_url329": auth_dict["Jawa Timur"]["Kabupaten Sumenep4"],
            "dprd_url330": auth_dict["Jawa Timur"]["Kabupaten Trenggalek1"],
            "dprd_url331": auth_dict["Jawa Timur"]["Kabupaten Trenggalek2"],
            "dprd_url332": auth_dict["Jawa Timur"]["Kabupaten Tuban1"],
            "dprd_url333": auth_dict["Jawa Timur"]["Kabupaten Tuban2"],
            "dprd_url334": auth_dict["Jawa Timur"]["Kabupaten Tulungagung"],
            "dprd_url335": auth_dict["Jawa Timur"]["Kota Batu"],
            "dprd_url336": auth_dict["Jawa Timur"]["Kota Blitar"],
            "dprd_url337": auth_dict["Jawa Timur"]["Kota Kediri"],
            "dprd_url338": auth_dict["Jawa Timur"]["Kota Madiun"],
            "dprd_url339": auth_dict["Jawa Timur"]["Kota Malang"],
            "dprd_url340": auth_dict["Jawa Timur"]["Kota Mojokerto"],
            "dprd_url341": auth_dict["Jawa Timur"]["Kota Pasuruan"],
            "dprd_url342": auth_dict["Jawa Timur"]["Kota Probolinggo"],
            "dprd_url343": auth_dict["Jawa Timur"]["Kota Surabaya"],
            "dprd_url344": auth_dict["DKI Jakarta"]["Kota Administrasi Jakarta Barat"],
            "dprd_url345": auth_dict["DKI Jakarta"]["Kota Administrasi Jakarta Pusat"],
            "dprd_url346": auth_dict["DKI Jakarta"]["Kota Administrasi Jakarta Selatan"],
            "dprd_url347": auth_dict["DKI Jakarta"]["Kota Administrasi Jakarta Timur"],
            "dprd_url348": auth_dict["DKI Jakarta"]["Kota Administrasi Jakarta Utara"],
            "dprd_url349": auth_dict["DKI Jakarta"]["Kabupaten Administrasi Kepulauan Seribu"],
            "dprd_url350": auth_dict["Yogyakarta"]["Kabupaten Bantul1"],
            "dprd_url351": auth_dict["Yogyakarta"]["Kabupaten Bantul2"],
            "dprd_url352": auth_dict["Yogyakarta"]["Kabupaten Bantul3"],
            "dprd_url353": auth_dict["Yogyakarta"]["Kabupaten Bantul4"],
            "dprd_url354": auth_dict["Yogyakarta"]["Kabupaten Gunungkidul"],
            "dprd_url355": auth_dict["Yogyakarta"]["Kabupaten Kulon Progo1"],
            "dprd_url356": auth_dict["Yogyakarta"]["Kabupaten Kulon Progo2"],
            "dprd_url357": auth_dict["Yogyakarta"]["Kabupaten Sleman"],
            "dprd_url358": auth_dict["Yogyakarta"]["Kota Yogyakarta"],
            "dprd_url359": auth_dict["Bali"]["Kabupaten Badung1"],
            "dprd_url360 ": auth_dict["Bali"]["Kabupaten Badung2"],
            "dprd_url361": auth_dict["Bali"]["Kabupaten Bangli"],
            "dprd_url362": auth_dict["Bali"]["Kabupaten Buleleng"],
            "dprd_url363": auth_dict["Bali"]["Kabupaten Gianyar"],
            "dprd_url364": auth_dict["Bali"]["Kabupaten Jembrana"],
            "dprd_url365": auth_dict["Bali"]["Kabupaten Karangasem"],
            "dprd_url366": auth_dict["Bali"]["Kabupaten Klungkung"],
            "dprd_url367": auth_dict["Bali"]["Kabupaten Tabanan1"],
            "dprd_url368": auth_dict["Bali"]["Kabupaten Tabanan2"],
            "dprd_url369": auth_dict["Bali"]["Kota Denpasar"],
            "dprd_url370": auth_dict["NTB"]["Kabupaten Bima"],
            "dprd_url371": auth_dict["NTB"]["Kabupaten Dompu"],
            "dprd_url372": auth_dict["NTB"]["Kabupaten Lombok Barat"],
            "dprd_url373": auth_dict["NTB"]["Kabupaten Lombok Tengah"],
            "dprd_url374": auth_dict["NTB"]["Kabupaten Lombok Timur"],
            "dprd_url375": auth_dict["NTB"]["Kabupaten Lombok Utara"],
            "dprd_url376": auth_dict["NTB"]["Kabupaten Sumbawa"],
            "dprd_url377": auth_dict["NTB"]["Kabupaten Sumbawa Barat"],
            "dprd_url378": auth_dict["NTB"]["Kota Bima"],
            "dprd_url379": auth_dict["NTB"]["Kota Mataram"],
            "dprd_url380": auth_dict["NTT"]["Kabupaten Alor"],
            "dprd_url381": auth_dict["NTT"]["Kabupaten Belu"],
            "dprd_url382": auth_dict["NTT"]["Kabupaten Ende"],
            "dprd_url383": auth_dict["NTT"]["Kabupaten Flores Timur"],
            "dprd_url384": auth_dict["NTT"]["Kabupaten Kupang"],
            "dprd_url385": auth_dict["NTT"]["Kabupaten Lembata"],
            "dprd_url386": auth_dict["NTT"]["Kabupaten Malaka"],
            "dprd_url387": auth_dict["NTT"]["Kabupaten Manggarai"],
            "dprd_url388": auth_dict["NTT"]["Kabupaten Manggarai Barat"],
            "dprd_url389": auth_dict["NTT"]["Kabupaten Manggarai Timur"],
            "dprd_url390": auth_dict["NTT"]["Kabupaten Ngada"],
            "dprd_url391": auth_dict["NTT"]["Kabupaten Nagekeo"],
            "dprd_url392": auth_dict["NTT"]["Kabupaten Rote Ndao"],
            "dprd_url393": auth_dict["NTT"]["Kabupaten Sabu Raijua"],
            "dprd_url394": auth_dict["NTT"]["Kabupaten Sikka"],
            "dprd_url395": auth_dict["NTT"]["Kabupaten Sumba Barat"],
            "dprd_url396": auth_dict["NTT"]["Kabupaten Sumba Barat Daya"],
            "dprd_url397": auth_dict["NTT"]["Kabupaten Sumba Tengah"],
            "dprd_url398": auth_dict["NTT"]["Kabupaten Sumba Timur"],
            "dprd_url399": auth_dict["NTT"]["Kabupaten Timor Tengah Selatan"],
            "dprd_url400": auth_dict["NTT"]["Kabupaten Timor Tengah Utara"],
            "dprd_url401": auth_dict["NTT"]["Kota Kupang"],
            "dprd_url402": auth_dict["KALBAR"]["Kabupaten Bengkayang"],
            "dprd_url403": auth_dict["KALBAR"]["Kabupaten Kapuas Hulu"],
            "dprd_url404": auth_dict["KALBAR"]["Kabupaten Kayong Utara"],
            "dprd_url405": auth_dict["KALBAR"]["Kabupaten Ketapang"],
            "dprd_url406": auth_dict["KALBAR"]["Kabupaten Kubu Raya"],
            "dprd_url407": auth_dict["KALBAR"]["Kabupaten Landak"],
            "dprd_url408": auth_dict["KALBAR"]["Kabupaten Melawi"],
            "dprd_url409": auth_dict["KALBAR"]["Kabupaten Mempawah"],
            "dprd_url410": auth_dict["KALBAR"]["Kabupaten Sambas"],
            "dprd_url411": auth_dict["KALBAR"]["Kabupaten Sanggau"],
            "dprd_url412": auth_dict["KALBAR"]["Kabupaten Sekadau"],
            "dprd_url413": auth_dict["KALBAR"]["Kabupaten Sintang"],
            "dprd_url414": auth_dict["KALBAR"]["Kota Pontianak"],
            "dprd_url415": auth_dict["KALBAR"]["Kota Singkawang"],
            "dprd_url416": auth_dict["KALSEL"]["Kabupaten Balangan"],
            "dprd_url417": auth_dict["KALSEL"]["Kabupaten Banjar"],
            "dprd_url418": auth_dict["KALSEL"]["Kabupaten Barito Kuala"],
            "dprd_url419": auth_dict["KALSEL"]["Kabupaten Hulu Sungai Selatan"],
            "dprd_url420": auth_dict["KALSEL"]["Kabupaten Hulu Sungai Tengah"],
            "dprd_url421": auth_dict["KALSEL"]["Kabupaten Hulu Sungai Utara"],
            "dprd_url422": auth_dict["KALSEL"]["Kabupaten Kotabaru"],
            "dprd_url423": auth_dict["KALSEL"]["Kabupaten Tabalong"],
            "dprd_url424": auth_dict["KALSEL"]["Kabupaten Tanah Bumbu"],
            "dprd_url425": auth_dict["KALSEL"]["Kabupaten Tanah Laut"],
            "dprd_url426": auth_dict["KALSEL"]["Kabupaten Tapin"],
            "dprd_url427": auth_dict["KALSEL"]["Kota Banjarbaru"],
            "dprd_url428": auth_dict["KALSEL"]["Kota Banjarmasin1"],
            "dprd_url429": auth_dict["KALSEL"]["Kota Banjarmasin2"],
            "dprd_url430": auth_dict["KALSEL"]["Kota Banjarmasin3"],
            "dprd_url431": auth_dict["KALSEL"]["Kota Banjarmasin4"],
            "dprd_url432": auth_dict["KALSEL"]["Kota Banjarmasin5"],
            "dprd_url433": auth_dict["KALSEL"]["Kota Banjarmasin6"],
            "dprd_url434": auth_dict["KALSEL"]["Kota Banjarmasin7"],
            "dprd_url435": auth_dict["KALSEL"]["Kota Banjarmasin8"],
            "dprd_url436": auth_dict["KALTENG"]["Kabupaten Barito Selatan"],
            "dprd_url437": auth_dict["KALTENG"]["Kabupaten Barito Timur"],
            "dprd_url438": auth_dict["KALTENG"]["Kabupaten Barito Utara"],
            "dprd_url439": auth_dict["KALTENG"]["Kabupaten Gunung Mas"],
            "dprd_url440": auth_dict["KALTENG"]["Kabupaten Kapuas"],
            "dprd_url441": auth_dict["KALTENG"]["Kabupaten Katingan"],
            "dprd_url442": auth_dict["KALTENG"]["Kabupaten Kotawaringin Barat"],
            "dprd_url443": auth_dict["KALTENG"]["Kabupaten Kotawaringin Timur"],
            "dprd_url444": auth_dict["KALTENG"]["Kabupaten Lamandau"],
            "dprd_url445": auth_dict["KALTENG"]["Kabupaten Murung Raya"],
            "dprd_url446": auth_dict["KALTENG"]["Kabupaten Pulang Pisau"],
            "dprd_url447": auth_dict["KALTENG"]["Kabupaten Sukamara"],
            "dprd_url448": auth_dict["KALTENG"]["Kabupaten Seruyan"],
            "dprd_url449": auth_dict["KALTENG"]["Kota Palangka Raya"],
            "dprd_url450": auth_dict["KALTIM"]["Kabupaten Berau"],
            "dprd_url451": auth_dict["KALTIM"]["Kabupaten Kutai Barat"],
            "dprd_url452": auth_dict["KALTIM"]["Kabupaten Kutai Kartanegara"],
            "dprd_url453": auth_dict["KALTIM"]["Kabupaten Kutai Timur"],
            "dprd_url454": auth_dict["KALTIM"]["Kabupaten Mahakam Ulu"],
            "dprd_url455": auth_dict["KALTIM"]["Kabupaten Paser"],
            "dprd_url456": auth_dict["KALTIM"]["Kabupaten Penajam Paser Utara"],
            "dprd_url457": auth_dict["KALTIM"]["Kota Balikpapan"],
            "dprd_url458": auth_dict["KALTIM"]["Kota Bontang"],
            "dprd_url459": auth_dict["KALTIM"]["Kota Samarinda1"],
            "dprd_url460": auth_dict["KALTIM"]["Kota Samarinda2"],
            "dprd_url461": auth_dict["KALTIM"]["Kota Samarinda3"],
            "dprd_url462": auth_dict["KALTIM"]["Kota Samarinda4"],
            "dprd_url463": auth_dict["KALTIM"]["Kota Samarinda5"],
            "dprd_url464": auth_dict["KALTIM"]["Kota Samarinda6"],
            "dprd_url465": auth_dict["KALTIM"]["Kota Samarinda7"],
            "dprd_url466": auth_dict["KALTIM"]["Kota Samarinda8"],
            "dprd_url467": auth_dict["KALTARA"]["Kabupaten Bulungan"],
            "dprd_url468": auth_dict["KALTARA"]["Kabupaten Malinau"],
            "dprd_url469": auth_dict["KALTARA"]["Kabupaten Nunukan"],
            "dprd_url470": auth_dict["KALTARA"]["Kabupaten Tana Tidung"],
            "dprd_url471": auth_dict["KALTARA"]["Kota Tarakan"],
            "dprd_url472": auth_dict["Gorontalo"]["Kabupaten Boalemo"],
            "dprd_url473": auth_dict["Gorontalo"]["Kabupaten Bone Bolango"],
            "dprd_url474": auth_dict["Gorontalo"]["Kabupaten Gorontalo1"],
            "dprd_url475": auth_dict["Gorontalo"]["Kabupaten Gorontalo2"],
            "dprd_url476": auth_dict["Gorontalo"]["Kabupaten Gorontalo3"],
            "dprd_url477": auth_dict["Gorontalo"]["Kabupaten Gorontalo4"],
            "dprd_url478": auth_dict["Gorontalo"]["Kabupaten Gorontalo5"],
            "dprd_url479": auth_dict["Gorontalo"]["Kabupaten Gorontalo6"],
            "dprd_url480": auth_dict["Gorontalo"]["Kabupaten Gorontalo Utara"],
            "dprd_url481": auth_dict["Gorontalo"]["Kabupaten Pohuwato"],
            "dprd_url482": auth_dict["Gorontalo"]["Kota Gorontalo"],
            "dprd_url483": auth_dict["SULSEL"]["Kabupaten Bantaeng"],
            "dprd_url484": auth_dict["SULSEL"]["Kabupaten Barru"],
            "dprd_url485": auth_dict["SULSEL"]["Kabupaten Bone"],
            "dprd_url486": auth_dict["SULSEL"]["Kabupaten Bulukumba"],
            "dprd_url487": auth_dict["SULSEL"]["Kabupaten Enrekang"],
            "dprd_url488": auth_dict["SULSEL"]["Kabupaten Gowa"],
            "dprd_url489": auth_dict["SULSEL"]["Kabupaten Jeneponto"],
            "dprd_url490": auth_dict["SULSEL"]["Kabupaten Kepulauan Selayar"],
            "dprd_url491": auth_dict["SULSEL"]["Kabupaten Luwu"],
            "dprd_url492": auth_dict["SULSEL"]["Kabupaten Luwu Timur"],
            "dprd_url493": auth_dict["SULSEL"]["Kabupaten Luwu Utara"],
            "dprd_url494": auth_dict["SULSEL"]["Kabupaten Maros"],
            "dprd_url495": auth_dict["SULSEL"]["Kabupaten Pangkajene dan Kepulauan1"],
            "dprd_url496": auth_dict["SULSEL"]["Kabupaten Pangkajene dan Kepulauan2"],
            "dprd_url497": auth_dict["SULSEL"]["Kabupaten Pangkajene dan Kepulauan3"],
            "dprd_url498": auth_dict["SULSEL"]["Kabupaten Pangkajene dan Kepulauan4"],
            "dprd_url499": auth_dict["SULSEL"]["Kabupaten Pangkajene dan Kepulauan5"],
            "dprd_url500": auth_dict["SULSEL"]["Kabupaten Pinrang"],
            "dprd_url501": auth_dict["SULSEL"]["Kabupaten Sidenreng Rappang"],
            "dprd_url502": auth_dict["SULSEL"]["Kabupaten Sinjai"],
            "dprd_url503": auth_dict["SULSEL"]["Kabupaten Soppeng"],
            "dprd_url504": auth_dict["SULSEL"]["Kabupaten Takalar"],
            "dprd_url505": auth_dict["SULSEL"]["Kabupaten Tana Toraja"],
            "dprd_url506": auth_dict["SULSEL"]["Kabupaten Toraja Utara"],
            "dprd_url507": auth_dict["SULSEL"]["Kabupaten Wajo"],
            "dprd_url508": auth_dict["SULSEL"]["Kota Makassar 1"],
            "dprd_url509": auth_dict["SULSEL"]["Kota Palopo"],
            "dprd_url510": auth_dict["SULSEL"]["Kota Parepare"],
            "dprd_url511": auth_dict["SULTRA"]["Kabupaten Bombana"],
            "dprd_url512": auth_dict["SULTRA"]["Kabupaten Buton"],
            "dprd_url513": auth_dict["SULTRA"]["Kabupaten Buton Selatan"],
            "dprd_url514": auth_dict["SULTRA"]["Kabupaten Buton Tengah"],
            "dprd_url515": auth_dict["SULTRA"]["Kabupaten Buton Utara"],
            "dprd_url516": auth_dict["SULTRA"]["Kabupaten Kolaka"],
            "dprd_url517": auth_dict["SULTRA"]["Kabupaten Kolaka Timur"],
            "dprd_url518": auth_dict["SULTRA"]["Kabupaten Kolaka Utara"],
            "dprd_url519": auth_dict["SULTRA"]["Kabupaten Konawe"],
            "dprd_url520": auth_dict["SULTRA"]["Kabupaten Konawe Kepulauan"],
            "dprd_url521": auth_dict["SULTRA"]["Kabupaten Konawe Selatan"],
            "dprd_url522": auth_dict["SULTRA"]["Kabupaten Konawe Utara"],
            "dprd_url523": auth_dict["SULTRA"]["Kabupaten Muna"],
            "dprd_url524": auth_dict["SULTRA"]["Kabupaten Muna Barat"],
            "dprd_url525": auth_dict["SULTRA"]["Kabupaten Wakatobi"],
            "dprd_url526": auth_dict["SULTRA"]["Kota Bau-Bau"],
            "dprd_url527": auth_dict["SULTRA"]["Kota Kendari"],
            "dprd_url528": auth_dict["SULTENG"]["Kabupaten Banggai"],
            "dprd_url529": auth_dict["SULTENG"]["Kabupaten Banggai Kepulauan"],
            "dprd_url530": auth_dict["SULTENG"]["Kabupaten Banggai Laut"],
            "dprd_url531": auth_dict["SULTENG"]["Kabupaten Buol"],
            "dprd_url532": auth_dict["SULTENG"]["Kabupaten Donggala"],
            "dprd_url533": auth_dict["SULTENG"]["Kabupaten Morowali"],
            "dprd_url534": auth_dict["SULTENG"]["Kabupaten Morowali Utara"],
            "dprd_url535": auth_dict["SULTENG"]["Kabupaten Parigi Moutong"],
            "dprd_url536": auth_dict["SULTENG"]["Kabupaten Poso"],
            "dprd_url537": auth_dict["SULTENG"]["Kabupaten Sigi"],
            "dprd_url538": auth_dict["SULTENG"]["Kabupaten Tojo Una-Una"],
            "dprd_url539": auth_dict["SULTENG"]["Kabupaten Toli-Toli"],
            "dprd_url540": auth_dict["SULTENG"]["Kota Palu"],
            "dprd_url541": auth_dict["SULUT"]["Kabupaten Bolaang Mongondow Selatan"],
            "dprd_url542": auth_dict["SULUT"]["Kabupaten Bolaang Mongondow Timur"],
            "dprd_url543": auth_dict["SULUT"]["Kabupaten Bolaang Mongondow Utara"],
            "dprd_url544": auth_dict["SULUT"]["Kabupaten Kepulauan Sangihe"],
            "dprd_url545": auth_dict["SULUT"]["Kabupaten Kepulauan Siau Tagulandang Biaro"],
            "dprd_url546": auth_dict["SULUT"]["Kabupaten Kepulauan Talaud"],
            "dprd_url547": auth_dict["SULUT"]["Kabupaten Minahasa"],
            "dprd_url548": auth_dict["SULUT"]["Kabupaten Minahasa Selatan"],
            "dprd_url549": auth_dict["SULUT"]["Kabupaten Minahasa Tenggara"],
            "dprd_url550": auth_dict["SULUT"]["Kabupaten Minahasa Utara"],
            "dprd_url551": auth_dict["SULUT"]["Kota Bitung"],
            "dprd_url552": auth_dict["SULUT"]["Kota Kotamobagu"],
            "dprd_url553": auth_dict["SULUT"]["Kota Manado"],
            "dprd_url554": auth_dict["SULUT"]["Kota Tomohon"],
            "dprd_url555": auth_dict["SULBAR"]["Kabupaten Majene"],
            "dprd_url556": auth_dict["SULBAR"]["Kabupaten Mamasa"],
            "dprd_url557": auth_dict["SULBAR"]["Kabupaten Mamuju"],
            "dprd_url558": auth_dict["SULBAR"]["Kabupaten Mamuju Tengah"],
            "dprd_url559": auth_dict["SULBAR"]["Kabupaten Mamuju Utara"],
            "dprd_url560": auth_dict["SULBAR"]["Kabupaten Polewali Mandar"],
            "dprd_url561": auth_dict["SULBAR"]["Kota Mamuju"],
            "dprd_url562": auth_dict["Maluku"]["Kabupaten Buru"],
            "dprd_url563": auth_dict["Maluku"]["Kabupaten Buru Selatan"],
            "dprd_url564": auth_dict["Maluku"]["Kabupaten Kepulauan Aru"],
            "dprd_url565": auth_dict["Maluku"]["Kabupaten Maluku Barat Daya"],
            "dprd_url566": auth_dict["Maluku"]["Kabupaten Maluku Tengah"],
            "dprd_url567": auth_dict["Maluku"]["Kabupaten Maluku Tenggara"],
            "dprd_url568": auth_dict["Maluku"]["Kabupaten Maluku Tenggara Barat"],
            "dprd_url569": auth_dict["Maluku"]["Kabupaten Seram Bagian Barat"],
            "dprd_url570": auth_dict["Maluku"]["Kabupaten Seram Bagian Timur"],
            "dprd_url571": auth_dict["Maluku"]["Kota Ambon"],
            "dprd_url672": auth_dict["Maluku"]["Kota Tual"],
            "dprd_url573": auth_dict["Maluku Utara"]["Kabupaten Halmahera Barat"],
            "dprd_url574": auth_dict["Maluku Utara"]["Kabupaten Halmahera Tengah"],
            "dprd_url575": auth_dict["Maluku Utara"]["Kabupaten Halmahera Utara"],
            "dprd_url576": auth_dict["Maluku Utara"]["Kabupaten Halmahera Selatan"],
            "dprd_url577": auth_dict["Maluku Utara"]["Kabupaten Kepulauan Sula"],
            "dprd_url578": auth_dict["Maluku Utara"]["Kabupaten Halmahera Timur"],
            "dprd_url579": auth_dict["Maluku Utara"]["Kabupaten Pulau Morotai"],
            "dprd_url580": auth_dict["Maluku Utara"]["Kabupaten Pulau Taliabu"],
            "dprd_url581": auth_dict["Maluku Utara"]["Kota Ternate"],
            "dprd_url582": auth_dict["Maluku Utara"]["Kota Tidore Kepulauan"],
            "dprd_url583": auth_dict["Papua"]["Kabupaten Asmat"],
            "dprd_url584": auth_dict["Papua"]["Kabupaten Biak Numfor"],
            "dprd_url585": auth_dict["Papua"]["Kabupaten Boven Digoel"],
            "dprd_url586": auth_dict["Papua"]["Kabupaten Deiyai"],
            "dprd_url587": auth_dict["Papua"]["Kabupaten Dogiyai"],
            "dprd_url588": auth_dict["Papua"]["Kabupaten Intan Jaya"],
            "dprd_url589": auth_dict["Papua"]["Kabupaten Jayapura"],
            "dprd_url590": auth_dict["Papua"]["Kabupaten Jayawijaya"],
            "dprd_url591": auth_dict["Papua"]["Kabupaten Keerom"],
            "dprd_url592": auth_dict["Papua"]["Kabupaten Kepulauan Yapen"],
            "dprd_url593": auth_dict["Papua"]["Kabupaten Lanny Jaya"],
            "dprd_url594": auth_dict["Papua"]["Kabupaten Mamberamo Raya"],
            "dprd_url595": auth_dict["Papua"]["Kabupaten Mamberamo Tengah"],
            "dprd_url596": auth_dict["Papua"]["Kabupaten Mappi"],
            "dprd_url597": auth_dict["Papua"]["Kabupaten Merauke"],
            "dprd_url598": auth_dict["Papua"]["Kabupaten Mimika"],
            "dprd_url599": auth_dict["Papua"]["Kabupaten Nabire"],
            "dprd_url600": auth_dict["Papua"]["Kabupaten Nduga"],
            "dprd_url601": auth_dict["Papua"]["Kabupaten Paniai"],
            "dprd_url602": auth_dict["Papua"]["Kabupaten Pegunungan Bintang"],
            "dprd_url603": auth_dict["Papua"]["Kabupaten Puncak"],
            "dprd_url604": auth_dict["Papua"]["Kabupaten Puncak Jaya"],
            "dprd_url605": auth_dict["Papua"]["Kabupaten Sarmi"],
            "dprd_url606": auth_dict["Papua"]["Kabupaten Supiori"],
            "dprd_url607": auth_dict["Papua"]["Kabupaten Tolikara"],
            "dprd_url607": auth_dict["Papua"]["Kabupaten Waropen"],
            "dprd_url608": auth_dict["Papua"]["Kabupaten Yahukimo"],
            "dprd_url609": auth_dict["Papua"]["Kabupaten Yalimo"],
            "dprd_url610": auth_dict["Papua"]["Kota Jayapura"],
            "dprd_url611": auth_dict["Papua Barat"]["Kabupaten Fakfak"],
            "dprd_url612": auth_dict["Papua Barat"]["Kabupaten Kaimana"],
            "dprd_url613": auth_dict["Papua Barat"]["Kabupaten Manokwari"],
            "dprd_url614": auth_dict["Papua Barat"]["Kabupaten Manokwari Selatan"],
            "dprd_url615": auth_dict["Papua Barat"]["Kabupaten Maybrat"],
            "dprd_url616": auth_dict["Papua Barat"]["Kabupaten Pegunungan Arfak"],
            "dprd_url617": auth_dict["Papua Barat"]["Kabupaten Raja Ampat"],
            "dprd_url618": auth_dict["Papua Barat"]["Kabupaten Sorong"],
            "dprd_url619": auth_dict["Papua Barat"]["Kabupaten Sorong Selatan"],
            "dprd_url620": auth_dict["Papua Barat"]["Kabupaten Tambrauw"],
            "dprd_url621": auth_dict["Papua Barat"]["Kabupaten Teluk Bintuni"],
            "dprd_url622": auth_dict["Papua Barat"]["Kabupaten Teluk Wondama"]
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

    def get_table(self, url):
        df = pd.read_html(url)[4]    
        df = df.rename(columns={"Nama Anggota" : "nama"})
        df["alamat"] = "No Data"
        df = df[["nama", "alamat"]]
        return df


    def aceh_barat(self):
        try:
            df = pd.read_html(self.dprd_url1)[5]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_barat.csv", index=False)
                self.success_list.append("aceh_barat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_barat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            #self.failed_list.append("\n")
            self.failed_list.append("aceh_barat failed..\n\n")
    

    def aceh_baratdaya(self):
        try:
            df = pd.read_html(self.dprd_url2)[6]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_barat_daya.csv", index=False)
                self.success_list.append("aceh_baratdaya success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_baratdaya failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_baratdaya failed..\n\n")
            


    def aceh_besar(self):
        try:
            df = pd.read_html(self.dprd_url3)[6]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_besar.csv", index=False)
                self.success_list.append("aceh_baratbesar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_baratbesar failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_baratbesar failed..\n\n")


    def aceh_jaya(self):
        try:
            df = pd.read_html(self.dprd_url4)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_jaya.csv", index=False)
                self.success_list.append("aceh_jaya success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_jaya failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_jaya failed..\n\n")


    def aceh_selatan(self):
        try:
            df = pd.read_html(self.dprd_url5)[5]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_selatan.csv", index=False)
                self.success_list.append("aceh_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_selatan failed..\n\n")
            
    

    def aceh_singkil(self):
        try:
            df = pd.read_html(self.dprd_url6)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_singkil.csv", index=False)
                self.success_list.append("aceh_singkil success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_singkil failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_singkil failed..\n\n")


    def aceh_tamiang(self):
        try:
            df = pd.read_html(self.dprd_url7)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_tamiang.csv", index=False)
                self.success_list.append("aceh_tamiang success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_tamiang failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_tamiang failed..\n\n")

        
    def aceh_tengah(self):
        try:
            df = pd.read_html(self.dprd_url8)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_tengah.csv", index=False)
                self.success_list.append("aceh_tengah success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_tengah failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_tengah failed..\n\n")


    def aceh_tenggara(self):
        try:
            df = pd.read_html(self.dprd_url9)[5]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_tenggara.csv", index=False)
                self.success_list.append("aceh_tenggara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_tenggara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_tenggara failed..\n\n")
    

    def aceh_timur(self):
        try:
            df = pd.read_html(self.dprd_url10)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_timur.csv", index=False)
                self.success_list.append("aceh_timur success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_timur failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_timur failed..\n\n")


    def aceh_utara(self):
        try:
            df = pd.read_html(self.dprd_url11)[5]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Aceh_utara.csv", index=False)
                self.success_list.append("aceh_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("aceh_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("aceh_utara failed..\n\n")

        
    def bener_meriah(self):
        try:
            df = pd.read_html(self.dprd_url12)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Bener_meriah.csv", index=False)
                self.success_list.append("bener_meriah success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bener_meriah failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bener_meriah failed..\n\n")


    def bireuen(self):
        try:
            df = pd.read_html(self.dprd_url13)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/bireuen.csv", index=False)
                self.success_list.append("bireuen success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bireuen failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bireuen failed..\n\n")
    

    def gayo_lues(self):
        try:
            df = pd.read_html(self.dprd_url14)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Gayo_lues.csv", index=False)
                self.success_list.append("gayo_lues success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("gayo_lues failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("gayo_lues failed..\n\n")


    def nagan_raya(self):
        try:
            df = pd.read_html(self.dprd_url15)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Nagan_raya.csv", index=False)
                self.success_list.append("nagan_raya success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("nagan_raya failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("nagan_raya failed..\n\n")

        
    def pidie(self):
        try:
            df = pd.read_html(self.dprd_url16)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Pidie.csv", index=False)
                self.success_list.append("pidie success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pidie failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pidie failed..\n\n")


    def pidie_jaya(self):
        try:
            df = pd.read_html(self.dprd_url17)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Pidie_jaya.csv", index=False)
                self.success_list.append("pidie_jaya success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pidie_jaya failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pidie_jaya failed..\n\n")
    

    def simeulue(self):
        try:
            df = pd.read_html(self.dprd_url18)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Simeulue.csv", index=False)
                self.success_list.append("simeulue success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("simeulue failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("simeulue failed..\n\n")


    def banda_aceh(self):
        try:
            df = pd.read_html(self.dprd_url19)[5]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Banda_aceh.csv", index=False)
                self.success_list.append("banda_aceh success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("banda_aceh failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("banda_aceh failed..\n\n")

        
    def langsa(self):
        try:
            df = pd.read_html(self.dprd_url20)[6]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Langsa.csv", index=False)
                self.success_list.append("langsa success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("langsa failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("langsa failed..\n\n")


    def lhokseumawe(self):
        try:
            df = pd.read_html(self.dprd_url21)[7]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Lhokseumawe.csv", index=False)
                self.success_list.append("lhokseumawe success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("lhokseumawe failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("lhokseumawe failed..\n\n")
    

    def sabang(self):
        try:
            df = pd.read_html(self.dprd_url22)[6]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Sabang.csv", index=False)
                self.success_list.append("sabang success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sabang failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sabang failed..\n\n")


    def subulussalam(self):
        try:
            df = pd.read_html(self.dprd_url23)[5]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Aceh/Subulussalam.csv", index=False)
                self.success_list.append("subulussalam success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("subulussalam failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("subulussalam failed..\n\n")

        
    def asahan(self):
        try:
            urls = [self.dprd_url24]
            li = []
            for url in tqdm(self.urls):
                df = pd.read_html(url)[0]
                print(df.shape)
                li.append(df)
            df = pd.concat(li)
            new_header = df.iloc[0] #grab the first row for the header
            df = df[1:] #take the data less the header row
            df.columns = new_header #set the header row as the df header
            
            df = df.rename(columns={"NAMA" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]

            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/Asahan.csv", index=False)
                self.success_list.append("asahan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("asahan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("asahan failed..\n\n")


    def batubara(self):
        try:
            urls = [self.dprd_url27, self.dprd_url28, self.dprd_url29, self.dprd_url30, self.dprd_url31, 
                    self.dprd_url32, self.dprd_url33, self.dprd_url34, self.dprd_url35, self.dprd_url36]

            list_nama = []
            for url in urls:
                soup = self.get_url(url)
                spans = soup.find_all("span", {"class" : "thumb-info-inner"})
                for span in spans:
                    list_nama.append(span.string)
            df = pd.DataFrame({"nama" : list_nama})
            df["alamat"] = "No Data"
            
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/Batubara.csv", index=False)
                self.success_list.append("batubara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("batubara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("batubara failed..\n\n")
    

    def dairi(self):
        try:
            df = pd.read_html(self.dprd_url37)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/Aceh_barat.csv", index=False)
                self.success_list.append("dairi success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("dairi failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("dairi failed..\n\n")


    def deli_serdang(self):
        try:
            df = pd.read_html(self.dprd_url38)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/Deli_serdang.csv", index=False)
                self.success_list.append("deli_serdang success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("deli_serdang failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("deli_serdang failed..\n\n")

        
    def humbang_hasundutan(self):
        try:
            df = pd.read_html(self.dprd_url39)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/Humbang_hasudutan.csv", index=False)
                self.success_list.append("humbang_hasudutan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("humbang_hasudutan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("humbang_hasudutan failed..\n\n")


    def karo(self):
        try:
            df = pd.read_html(self.dprd_url40)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/karo.csv", index=False)
                self.success_list.append("karo success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("karo failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("karo failed..\n\n")
    

    def labuhanbatu(self):
        try:
            df = pd.read_html(self.dprd_url41)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/Labuhanbatu.csv", index=False)
                self.success_list.append("labuhanbatu success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("labuhanbatu failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("labuhanbatu failed..\n\n")


    def labuhanbatu_selatan(self):
        try:
            df = pd.read_html(self.dprd_url42)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/Labuhanbatu_selatan.csv", index=False)
                self.success_list.append("Labuhanbatu_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("Labuhanbatu_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("Labuhanbatu_selatan failed..\n\n")

    def labuhanbatu_utara(self):
        try:
            df = pd.read_html(self.dprd_url43)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/Labuhanbatu_utara.csv", index=False)
                self.success_list.append("Labuhanbatu_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("Labuhanbatu_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("Labuhanbatu_utara failed..\n\n")

    def langkat(self):
        try:
            df = pd.read_html(self.dprd_url44)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/langkat.csv", index=False)
                self.success_list.append("langkat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("langkat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("langkat failed..\n\n")


    def mandailing_natal(self):
        try:
            df = pd.read_html(self.dprd_url45)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/mandailing_natal.csv", index=False)
                self.success_list.append("mandailing_natal success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("mandailing_natal failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("mandailing_natal failed..\n\n")


    def nias(self):
        try:
            df = pd.read_html(self.dprd_url46)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/Nias.csv", index=False)
                self.success_list.append("nias success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("nias failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("nias failed..\n\n")

    def nias_barat(self):
        try:
            df = pd.read_html(self.dprd_url47)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/nias_barat.csv", index=False)
                self.success_list.append("nias_barat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("nias_barat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("nias_barat failed..\n\n")


    def nias_selatan(self):
        try:
            df = pd.read_html(self.dprd_url48)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/nias_selatan.csv", index=False)
                self.success_list.append("nias_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("nias_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("nias_selatan failed..\n\n")


    def nias_utara(self):
        try:
            df = pd.read_html(self.dprd_url49)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/nias_utara.csv", index=False)
                self.success_list.append("nias_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("nias_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("nias_utara failed..\n\n")
    

    def padang_lawas(self):
        try:
            df = pd.read_html(self.dprd_url50)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/padang_lawas.csv", index=False)
                self.success_list.append("padang_lawas success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("padang_lawas failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("padang_lawas success..\n")
    

    def padang_lawas_utara(self):
        try:
            df = pd.read_html(self.dprd_url51)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/padang_lawas_utara.csv", index=False)
                self.success_list.append("padang_lawas_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("padang_lawas_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("padang_lawas_utara failed..\n\n")


    def pakpak_bharat(self):
        try:
            df = pd.read_html(self.dprd_url52)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/pakpak_bharat.csv", index=False)
                self.success_list.append("pakpak_bharat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pakpak_bharat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pakpak_bharat failed..\n\n")


    def samosir(self):
        try:
            df = pd.read_html(self.dprd_url53)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/samosir.csv", index=False)
                self.success_list.append("samosir success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("samosir failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("samosir failed..\n\n")


    def serdang_bedagai(self):
        try:
            df = pd.read_html(self.dprd_url54)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/serdang_bedagai.csv", index=False)
                self.success_list.append("serdang_bedagai success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("serdang_bedagai failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("serdang_bedagai failed..\n\n")


    def simalungun(self):
        try:
            df = pd.read_html(self.dprd_url55)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/simalungun.csv", index=False)
                self.success_list.append("simalungun success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("simalungun failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("simalungun failed..\n\n")

    
    def tapanuli_selatan(self):
        try:
            df = pd.read_html(self.dprd_url56)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/tapanuli_selatan.csv", index=False)
                self.success_list.append("tapanuli_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tapanuli_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tapanuli_selatan failed..\n\n")


    def tapanuli_tengah(self):
        try:
            df = pd.read_html(self.dprd_url57)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/tapanuli_tengah.csv", index=False)
                self.success_list.append("tapanuli_tengah success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tapanuli_tengah failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tapanuli_tengah failed..\n\n")

    def tapanuli_utara(self):
        try:
            df = pd.read_html(self.dprd_url58)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/tapanuli_utara.csv", index=False)
                self.success_list.append("tapanuli_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tapanuli_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tapanuli_utara failed..\n\n")


    def toba_samosir(self):
        try:
            df = pd.read_html(self.dprd_url59)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/toba_samosir.csv", index=False)
                self.success_list.append("toba_samosir success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("toba_samosir failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("toba_samosir failed..\n\n")


    def binjai(self):
        try:
            df = pd.read_html(self.dprd_url60)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/binjai.csv", index=False)
                self.success_list.append("binjai success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("binjai failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("binjai failed..\n\n")


    def gunungsitoli(self):
        try:
            df = pd.read_html(self.dprd_url61)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/gunungsitoli.csv", index=False)
                self.success_list.append("gunungsitoli success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("gunungsitoli failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("gunungsitoli failed..\n\n")


    def medan(self):
        try:
            df = pd.read_html(self.dprd_url62)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/medan.csv", index=False)
                self.success_list.append("medan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("medan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("medan failed..\n\n")


    def padangsidempuan(self):
        try:
            df = pd.read_html(self.dprd_url63)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/padangsidempuan.csv", index=False)
                self.success_list.append("padangsidempuan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("padangsidempuan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("padangsidempuan failed..\n\n")


    def pematangsiantar(self):
        try:
            df = pd.read_html(self.dprd_url64)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/pematangsiantar.csv", index=False)
                self.success_list.append("pematangsiantar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pematangsiantar failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pematangsiantar failed..\n\n")


    def sibolga(self):
        try:
            df = pd.read_html(self.dprd_url65)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/sibolga.csv", index=False)
                self.success_list.append("sibolga success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sibolga failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sibolga failed..\n\n")


    def tanjungbalai(self):
        try:
            df = pd.read_html(self.dprd_url66)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/tanjungbalai.csv", index=False)
                self.success_list.append("tanjungbalai success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tanjungbalai failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tanjungbalai failed..\n\n")


    def tebingtinggi(self):
        try:
            df = pd.read_html(self.dprd_url67)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumut/tebingtinggi.csv", index=False)
                self.success_list.append("tebingtinggi success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tebingtinggi failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tebingtinggi failed..\n\n")


    # def agam(self):
    #     try:
    #         df = pd.read_html(self.dprd_url68)[4]
    #         df = df.rename(columns={"Nama Anggota" : "nama"})
    #         df["alamat"] = "No Data"
    #         df = df[["nama", "alamat"]]
    #         if df.shape[0] > 5:
    #             df.to_csv("./scrapping/result/dprd_tk2/Sumbar/agam.csv", index=False)
    #             self.success_list.append("agam success..\n")
    #             return df
    #         else:
    #             self.error_desc.append("df.shape[0] < 5\n")
    #             self.failed_list.append("agam failed..\n\n")
    #     except Exception as e:
    #         self.error_desc.append(e)
    #         self.failed_list.append("agam failed..\n\n")


    # def dharmasraya(self):
    #     try:
    #         df = pd.read_html(self.dprd_url68)[4]
    #         df = df.rename(columns={"Nama Anggota" : "nama"})
    #         df["alamat"] = "No Data"
    #         df = df[["nama", "alamat"]]
    #         if df.shape[0] > 5:
    #             df.to_csv("./scrapping/result/dprd_tk2/Sumbar/agam.csv", index=False)
    #             self.success_list.append("dharmasraya success..\n")
    #             return df
    #         else:
    #             self.error_desc.append("df.shape[0] < 5\n")
    #             self.failed_list.append("dharmasraya failed..\n\n")
    #     except Exception as e:
    #         self.error_desc.append(e)
    #         self.failed_list.append("dharmasraya failed..\n\n")


    def agam(self):
        try:
            df = self.get_table(self.dprd_url68)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/agam.csv", index=False)
                self.success_list.append("agam success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("agam failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("agam failed..\n\n")


    def dharmasraya(self):
        try:
            df = self.get_table(self.dprd_url69)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/dharmasraya.csv", index=False)
                self.success_list.append("dharmasraya success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("dharmasraya failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("dharmasraya failed..\n\n")


    def kep_mentawai(self):
        try:
            df = self.get_table(self.dprd_url70)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/kep_mentawai.csv", index=False)
                self.success_list.append("kep_mentawai success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kep_mentawai failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kep_mentawai failed..\n\n")


    def lima_puluh_kota(self):
        try:
            df = self.get_table(self.dprd_url71)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/lima_puluh_kota.csv", index=False)
                self.success_list.append("lima_puluh_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("lima_puluh_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("lima_puluh_kota failed..\n\n")


    def padang_pariaman(self):
        try:
            df = self.get_table(self.dprd_url72)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/padang_pariaman.csv", index=False)
                self.success_list.append("padang_pariaman success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("padang_pariaman failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("padang_pariaman failed..\n\n")


    def pasaman(self):
        try:
            df = self.get_table(self.dprd_url73)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/pasaman.csv", index=False)
                self.success_list.append("pasaman success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pasaman failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pasaman failed..\n\n")


    def pasaman_barat(self):
        try:
            df = self.get_table(self.dprd_url74)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/pasaman_barat.csv", index=False)
                self.success_list.append("pasaman_barat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pasaman_barat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pasaman_barat failed..\n\n")


    def pesisir_selatan(self):
        try:
            df = self.get_table(self.dprd_url75)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/pesisir_selatan.csv", index=False)
                self.success_list.append("pesisir_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pesisir_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pesisir_selatan failed..\n\n")


    def sijunjung(self):
        try:
            df = self.get_table(self.dprd_url76)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/sijunjung.csv", index=False)
                self.success_list.append("sijunjung success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sijunjung failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sijunjung failed..\n\n")


    def solok(self):
        try:
            df = self.get_table(self.dprd_url77)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/solok.csv", index=False)
                self.success_list.append("solok success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("solok failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("solok failed..\n\n")


    def solok_selatan(self):
        try:
            df = self.get_table(self.dprd_url78)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/solok_selatan.csv", index=False)
                self.success_list.append("solok_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("solok_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("solok_selatan failed..\n\n")


    def tanah_datar(self):
        try:
            df = self.get_table(self.dprd_url79)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/tanah_datar.csv", index=False)
                self.success_list.append("tanah_datar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tanah_datar failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tanah_datar failed..\n\n")


    def bukittinggi(self):
        try:
            df = pd.read_html(self.dprd_url80)[5]    
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/bukittinggi.csv", index=False)
                self.success_list.append("bukittinggi success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bukittinggi failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bukittinggi failed..\n\n")


    def padang(self):
        try:
            df = pd.read_html(self.dprd_url81)[5]    
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/padang.csv", index=False)
                self.success_list.append("padang success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("padang failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("padang failed..\n\n")


    def padangpanjang(self):
        try:
            df = self.get_table(self.dprd_url82)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/padangpanjang.csv", index=False)
                self.success_list.append("padangpanjang success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("padangpanjang failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("padangpanjang failed..\n\n")


    def pariaman(self):
        try:
            df = self.get_table(self.dprd_url83)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/pariaman.csv", index=False)
                self.success_list.append("pariaman success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pariaman failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pariaman failed..\n\n")


    def payakumbuh(self):
        try:
            df = self.get_table(self.dprd_url84)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/payakumbuh.csv", index=False)
                self.success_list.append("payakumbuh success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("payakumbuh failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("payakumbuh failed..\n\n")


    def sawahlunto(self):
        try:
            df = pd.read_html(self.dprd_url85)[5]    
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/sawahlunto.csv", index=False)
                self.success_list.append("sawahlunto success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sawahlunto failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sawahlunto failed..\n\n")


    def solok(self):
        try:
            df = self.get_table(self.dprd_url86)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumbar/solok.csv", index=False)
                self.success_list.append("solok success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("solok failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("solok failed..\n\n")


    def banyuasin(self):
        try:
            soup = self.get_url(self.dprd_url87)
            list_nama = []
            spans = soup.find_all("a", {"class":"fancybox"}, href=True)
            for span in spans:
                nama = str(span).split('title="')[1]
                list_nama.append(nama.split('"/><')[0])
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumsel/banyuasin.csv", index=False)
                self.success_list.append("banyuasin success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("banyuasin failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("banyuasin failed..\n\n")


    def empat_lawang(self):
        self.error_desc.append("no data")
        self.failed_list.append("empat lawang failed..\n\n")


    def lahat(self):
        try:
            soup = self.get_url(self.dprd_url89)
            spans = soup.find_all("ol")
            spans.find_all("li")
            list_nama = []
            for span in spans:
                for child in span.find_all("li"):
                    list_nama.append(child.text.split("/")[0])
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumsel/lahat.csv", index=False)
                self.success_list.append("lahat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("lahat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("lahat failed..\n\n")


    def muara_enim(self):
        self.error_desc.append("no data")
        self.failed_list.append("muara_enim failed..\n\n")


    def musi_banyuasin(self):
        self.error_desc.append("no data")
        self.failed_list.append("musi_banyuasin failed..\n\n")


    def musi_rawas(self):
        self.error_desc.append("no data")
        self.failed_list.append("musi_rawas failed..\n\n")


    def musi_rawas_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("musi_rawas_utara failed..\n\n")


    def ogan_ilir(self):
        try:
            soup = self.get_url(self.dprd_url94)
            list_nama = []
            for child in soup.find_all("div", {"class":"profilgroup-title"}):
                list_nama.append(child.text.replace("\r\n", "").strip())
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumsel/ogan_ilir.csv", index=False)
                self.success_list.append("ogan_ilir success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("ogan_ilir failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("ogan_ilir failed..\n\n")


    def ogan_komering_ilir(self):
        self.error_desc.append("no data")
        self.failed_list.append("ogan_komering_ilir failed..\n\n")
    

    def ogan_komering_ulu(self):
        self.error_desc.append("no data")
        self.failed_list.append("ogan_komering_ulu failed..\n\n")


    def ogan_komering_ulu_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("ogan_komering_ulu_selatan failed..\n\n")


    def ogan_komering_ulu_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("ogan_komering_ulu_timur failed..\n\n")


    def penukal_abab_lematang_ilir(self):
        self.error_desc.append("no data")
        self.failed_list.append("penukal_abab_lematang_ilir failed..\n\n")


    def lubuklinggau(self):
        self.error_desc.append("no data")
        self.failed_list.append("lubuklinggau failed..\n\n")


    def pagaralam(self):
        self.error_desc.append("no data")
        self.failed_list.append("pagaralam failed..\n\n")


    def palembang(self):
        self.error_desc.append("no data")
        self.failed_list.append("palembang failed..\n\n")


    def prabumulih(self):
        try:
            urls = [self.dprd_url103, self.dprd_url104, self.dprd_url105]
            nama_list = []
            for url in urls:
                soup = self.get_url(url)
                for span in soup.find_all("strong"):
                    if "NAMA" in span.text:
                        nama_list.append(span.text.split("NAMA : ")[1])
            df = pd.DataFrame.from_dict({"nama" : nama_list})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sumsel/prabumulih.csv", index=False)
                self.success_list.append("prabumulih success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("prabumulih failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("prabumulih failed..\n\n")
        

    def bengkalis(self):
        try:
            df = pd.read_html(self.dprd_url106)[5]    
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/bengkalis.csv", index=False)
                self.success_list.append("bengkalis success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bengkalis failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bengkalis failed..\n\n")


    def indragiri_hilir(self):
        try:
            df = self.get_table(self.dprd_url107)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/indragiri_hilir.csv", index=False)
                self.success_list.append("indragiri_hilir success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("indragiri_hilir failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("indragiri_hilir failed..\n\n")


    def indragiri_hulu(self):
        try:
            df = self.get_table(self.dprd_url108)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/indragiri_hulu.csv", index=False)
                self.success_list.append("indragiri_hulu success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("indragiri_hulu failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("indragiri_hulu failed..\n\n")


    def kampar(self):
        try:
            df = self.get_table(self.dprd_url109)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/kampar.csv", index=False)
                self.success_list.append("kampar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kampar failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kampar failed..\n\n")


    def kep_meranti(self):
        try:
            df = self.get_table(self.dprd_url110)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/kep_meranti.csv", index=False)
                self.success_list.append("kep_meranti success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kep_meranti failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kep_meranti failed..\n\n")


    def kuantan_singingi(self):
        try:
            df = self.get_table(self.dprd_url111)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/kuantan_singingi.csv", index=False)
                self.success_list.append("kuantan_singingi success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kuantan_singingi failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kuantan_singingi failed..\n\n")


    def pelalawan(self):
        try:
            df = self.get_table(self.dprd_url112)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/pelalawan.csv", index=False)
                self.success_list.append("pelalawan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pelalawan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pelalawan failed..\n\n")


    def rokan_hilir(self):
        try:
            df = self.get_table(self.dprd_url113)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/rokan_hilir.csv", index=False)
                self.success_list.append("rokan_hilir success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("rokan_hilir failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("rokan_hilir failed..\n\n")


    def rokan_hulu(self):
        try:
            df = self.get_table(self.dprd_url114)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/rokan_hulu.csv", index=False)
                self.success_list.append("rokan_hulu success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("rokan_hulu failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("rokan_hulu failed..\n\n")


    def siak(self):
        try:
            df = self.get_table(self.dprd_url115)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/siak.csv", index=False)
                self.success_list.append("siak success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("siak failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("siak failed..\n\n")


    def dumai(self):
        try:
            df = self.get_table(self.dprd_url116)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/dumai.csv", index=False)
                self.success_list.append("dumai success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("dumai failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("dumai failed..\n\n")


    def pekanbaru(self):
        try:
            df = self.get_table(self.dprd_url117)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Riau/pekanbaru.csv", index=False)
                self.success_list.append("pekanbaru success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pekanbaru failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pekanbaru failed..\n\n")


    def bintan(self):
        self.error_desc.append("no data")
        self.failed_list.append("bintan failed..\n\n")


    def karimun(self):
        try:
            # df = self.get_table()
            df = pd.read_html(self.dprd_url119)[0]
            new_cols = df.iloc[3]
            df.columns = new_cols
            df = df.iloc[4:34].reset_index(drop=True)
            df = df.rename(columns={"NAMA" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Kepulauan Riau/karimun.csv", index=False)
                self.success_list.append("karimun success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("karimun failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("karimun failed..\n\n")


    def kep_anambas(self):
        self.error_desc.append("no data")
        self.failed_list.append("kep_anambas failed..\n\n")


    def lingga(self):
        self.error_desc.append("no data")
        self.failed_list.append("lingga failed..\n\n")


    def natuna(self):
        self.error_desc.append("no data")
        self.failed_list.append("natuna failed..\n\n")


    def batam(self):
        self.error_desc.append("no data")
        self.failed_list.append("batam failed..\n\n")


    def tanjung_pinang(self):
        self.error_desc.append("no data")
        self.failed_list.append("tanjung_pinang failed..\n\n")


    def batang_hari(self):
        self.error_desc.append("no data")
        self.failed_list.append("tanjung_pinang failed..\n\n")


    def bungo(self):
        try:
            soup = self.get_url(self.dprd_url126)
            nama_list = []
            for span in soup.find_all("a", "be-fc-orange"):
                nama_list.append(span.text)
            df = pd.DataFrame.from_dict({"nama" : nama_list})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jambi/bungo.csv", index=False)
                self.success_list.append("bungo success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bungo failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bungo failed..\n\n")


    def kerinci(self):
        self.error_desc.append("no data")
        self.failed_list.append("kerinci failed..\n\n")


    def merangin(self):
        self.error_desc.append("no data")
        self.failed_list.append("merangin failed..\n\n")

    
    def muaro_jambi(self):
        self.error_desc.append("no data")
        self.failed_list.append("muaro_jambi failed..\n\n")
    

    def sarolangun(self):
        try:
            self.get_url(self.dprd_url130)
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sarolangun failed..\n\n")


    def tanjung_jabung_barat(self):
        try:
            df = pd.read_html(self.dprd_url131)[12]
            df = df.rename(columns={"Nama":"nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jambi/tanjung_jabung_barat.csv", index=False)
                self.success_list.append("tanjung_jabung_barat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tanjung_jabung_barat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tanjung_jabung_barat failed..\n\n")


    def tanjung_jabung_timur(self):
        try:
            df = pd.read_html(self.dprd_url132)[11]
            df = df.rename(columns={"Nama":"nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jambi/tanjung_jabung_timur.csv", index=False)
                self.success_list.append("tanjung_jabung_timur success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tanjung_jabung_timur failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tanjung_jabung_timur failed..\n\n")


    def tebo(self):
        self.error_desc.append("no data")
        self.failed_list.append("tebo failed..\n\n")


    def jambi(self):
        self.error_desc.append("no data")
        self.failed_list.append("jambi failed..\n\n")

    
    def sungai_penuh(self):
        self.error_desc.append("no data")
        self.failed_list.append("sungai_penuh failed..\n\n")


    def bungkulu_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("bengkulu_selatan failed..\n\n")


    def bengkulu_tengah(self):
        self.error_desc.append("no data")
        self.failed_list.append("bengkulu_tengah failed..\n\n")


    def bengkulu_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("bengkulu_utara failed..\n\n")

    def kaur(self):
        self.error_desc.append("no data")
        self.failed_list.append("kaur failed..\n\n")


    def kepahiang(self):
        self.error_desc.append("no data")
        self.failed_list.append("kepahiang failed..\n\n")


    def lebong(self):
        self.error_desc.append("no data")
        self.failed_list.append("lebong failed..\n\n")


    def muko_muko(self):
        self.error_desc.append("no data")
        self.failed_list.append("muko_muko failed..\n\n")


    def rejang_lebong(self):
        self.error_desc.append("no data")
        self.failed_list.append("rejang_lebong failed..\n\n")


    def seluma(self):
        self.error_desc.append("no data")
        self.failed_list.append("seluma failed..\n\n")


    def bengkulu(self):
        try:
            soup = self.get_url(self.dprd_url145)
            spans = soup.find_all("div", {"class":"elementor-element elementor-element-4b6d6e5 elementor-widget elementor-widget-text-editor"})
            list_nama = []
            for span in spans:
                a = span.find_all("p", {"style" : "text-align: center;"})
                for child in a:
                    extract_text = child.text
                    if extract_text.strip() == "":
                        pass
                    elif extract_text == "———————————————————————————":
                        pass
                    elif extract_text == "ANGGOTA":
                        pass
                    else:
                        list_nama.append(child.text)
            list_nama = list_nama[2:]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Bengkulu/bengkulu.csv", index=False)
                self.success_list.append("bengkulu success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bengkulu failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bengkulu failed..\n\n")


    def bangka(self):
        self.error_desc.append("no data")
        self.failed_list.append("bangka failed..\n\n")


    def bangka_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("bangka_barat failed..\n\n")


    def bangka_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("bangka_selatan failed..\n\n")


    def bangka_tengah(self):
        self.error_desc.append("no data")
        self.failed_list.append("bangka_tengah failed..\n\n")


    def belitung(self):
        self.error_desc.append("no data")
        self.failed_list.append("belitung failed..\n\n")


    def belitung_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("belitung_timur failed..\n\n")


    def pangkal_pinang(self):
        self.error_desc.append("no data")
        self.failed_list.append("pangkal_pinang failed..\n\n")


    def lampung_tengah(self):
        try:
            urls = [self.dprd_url153_1, self.dprd_url153_2, self.dprd_url153_3,
                    self.dprd_url153_4, self.dprd_url153_5, self.dprd_url153_6]
            list_link = []
            for url in urls:
                soup = self.get_url(url)
                for span in soup.find_all("div", {"class":"col-lg-4 col-sm-6 mb-25"}):
                    for child in span.find_all('a', href=True):
                        list_link.append(child["href"])
            li = []
            for url_child in list_link:
                url = self.dprd_url153_base + url_child
                soup = self.get_url(url)
                list_nama = []
                soup = self.get_url(url)
                list_nama.append(soup.find_all("figcaption")[0].find("h3").text)
                try:
                    df = pd.read_html(url)[0].T
                    df.columns = df.iloc[1]
                    df = df.iloc[[4]]
                    df["nama"] = list_nama
                    df = df.rename(columns={"Alamat" : "alamat"})
                    df["tempat lahir"] = [x[0] for x in df["Tempat, Tanggal Lahir"].str.split(",")]
                    df["tanggal lahir"] = [x[1] for x in df["Tempat, Tanggal Lahir"].str.split(",")]
                    df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]].reset_index(drop=True)
                except:
                    df = pd.DataFrame.from_dict({"nama" : list_nama})
                    df["alamat"] = "No Data"
                    df["tempat lahir"] = "No Data"
                    df["tanggal lahir"] = "No Data"
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                    df.to_csv("./scrapping/result/dprd_tk2/Lampung/lampung_tengah.csv", index=False)
                    self.success_list.append("lampung_tengah success..\n")
                    return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("lampung_tengah failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("lampung_tengah failed..\n\n")


    def lampung_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("lampung_utara failed..\n\n")


    def lampung_selatan(self):
        try:
            soup = self.get_url(self.dprd_url155)
            list_nama = []
            for span in soup.find("div", {"class" : "entry"}).find_all("li"):
                list_nama.append(span.text)
            df = pd.DataFrame.from_dict({"nama" : [x.split("(")[0] for x in list_nama]})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Lampung/lampung_selatan.csv", index=False)
                self.success_list.append("lampung_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("lampung_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("lampung_selatan failed..\n\n")


    def lampung_barat(self):
        try:
            urls = [self.dprd_url156_1, self.dprd_url156_2, 
                    self.dprd_url156_3, self.dprd_url156_4]            
            list_link = []
            for url in urls:
                soup = self.get_url(url)
                for span in soup.find("div", {"class" : "isi"}).find_all("a", href=True):
                    list_link.append(self.dprd_url156_base + span["href"])
            list_link = list(set(list_link))
            remove_ele = urls.copy()
            remove_ele.append('https://dprd-lampungbaratkab.go.id/')
            list_link = list(set(list_link) - set(remove_ele))

            li = []
            for child_url in tqdm(list_link):
                df = pd.read_html(child_url)[0].T
                df.columns = df.iloc[1]
                df = df.iloc[[3]]
                
                df["tempat lahir"] = [x[0] for x in df["Tempat Tgl Lahir"].str.split(",")]
                df["tanggal lahir"] = [x[1] for x in df["Tempat Tgl Lahir"].str.split(",")]
                df = df.rename(columns={"Nama" : "nama",
                                    "Alamat" : "alamat"})

                df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]].reset_index(drop=True)
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Lampung/Lampung_Barat.csv", index=False)
                self.success_list.append("lampung_barat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("lampung_barat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("lampung_barat failed..\n\n")


    def lampung_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("lampung_timur failed..\n\n")


    def mesuji(self):
        self.error_desc.append("no data")
        self.failed_list.append("mesuji failed..\n\n")


    def pesawaran(self):
        try:
            df = pd.read_html(self.dprd_url159)[4]
            df = df.rename(columns={"Nama Anggota" : "nama"})
            df["alamat"] = "No Data" 
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Lampung/pesawaran.csv", index=False)
                self.success_list.append("pesawaran success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pesawaran failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pesawaran failed..\n\n")


    def pesisir_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("pesisir_barat failed..\n\n")


    def pringsewu(self):
        self.error_desc.append("no data")
        self.failed_list.append("pringsewu failed..\n\n")


    def tulang_bawang(self):
        self.error_desc.append("no data")
        self.failed_list.append("tulang_bawang failed..\n\n")


    def tulang_bawang_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("tulang_bawang_barat failed..\n\n")


    def tanggamus(self):
        self.error_desc.append("no data")
        self.failed_list.append("tanggamus failed..\n\n")


    def way_kanan(self):
        self.error_desc.append("no data")
        self.failed_list.append("way_kanan failed..\n\n")


    def bandar_lampung(self):
        self.error_desc.append("no data")
        self.failed_list.append("bandar_lampung failed..\n\n")


    def metro_kota(self):
        try:
            df_list = pd.read_html(self.dprd_url167)
            li = []
            for child in tqdm(df_list):
                df = child.T
                df.columns = df.iloc[1]
                df = df.rename(columns={"Nama" : "nama",
                                    "Alamat" : "alamat"})
                df = df.iloc[[2]]
                df["tempat lahir"] = [x[0] for x in df["TTL"].str.split(",")]
                df["tanggal lahir"] = [x[1] for x in df["TTL"].str.split(",")]
                df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]].reset_index(drop=True)
                li.append(df) 
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Lampung/metro_kota.csv", index=False)
                self.success_list.append("metro_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("metro_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("metro_kota failed..\n\n")


    def lebak(self):
        self.error_desc.append("no data")
        self.failed_list.append("lebak failed..\n\n")


    def pandeglang(self):
        self.error_desc.append("no data")
        self.failed_list.append("pandeglang failed..\n\n")


    def serang(self):
        self.error_desc.append("no data")
        self.failed_list.append("serang failed..\n\n")


    def tangerang_kabupaten(self):
        try:
            df_list = pd.read_html(self.dprd_url171)
            li = []
            for child in tqdm(df_list):
                df = child
                df.columns = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, "nama", 11, 12, 13, 14, 15, 16]
                df = df[["nama"]]
                df["alamat"] = "No Data"
                df = df.iloc[5:-2]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Banten/tangerang_kabupaten.csv", index=False)
                self.success_list.append("tangerang_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tangerang_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tangerang_kabupaten failed..\n\n")


    def cilegon(self):
        self.error_desc.append("no data")
        self.failed_list.append("cilegon failed..\n\n")


    def serang(self):
        try:
            urls = [self.dprd_url173_1, self.dprd_url173_2, self.dprd_url173_3, 
                    self.dprd_url173_4, self.dprd_url173_5]
            list_link = []
            for url in urls:
                soup=self.get_url(url)
                for span in soup.find_all("div", {"class" : "details"}):
                    list_link.append(span.find("a", href=True)["href"])
            li = []
            for child in tqdm(list_link):
                soup = self.get_url(child)
                list_nama = []
                list_nama.append(soup.find("div", {"class" : "col-lg-6 details-left text-center"}).find("h2", 
                                                                                                        {"style" : "text-align:center"}).text)
                
                spans = soup.find("div", {"class" : "col-lg-6 details-right"}).find_all("li")
                ttl = spans[1].text.split(":")[1]
                tempat_lahir = [ttl.split(", ")[0].strip()]
                tanggal_lahir = [ttl.split(", ")[1].strip()]
                df = pd.DataFrame.from_dict({"nama" : list_nama,
                                            "alamat": ["No Data"],
                                        "tempat lahir" : tempat_lahir,
                                        "tanggal lahir" : tanggal_lahir})
                li.append(df)
            df = pd.concat(li, ignore_index=True)        
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Banten/serang.csv", index=False)
                self.success_list.append("serang success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("serang failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("serang failed..\n\n")


    def tangerang(self):
        self.error_desc.append("no data")
        self.failed_list.append("tangerang failed..\n\n")


    def tangerang_kabupaten(self):
        try:
            urls = [self.dprd_url175, self.dprd_url176, self.dprd_url177, 
                    self.dprd_url178, self.dprd_url179, self.dprd_url180, self.dprd_url181]
            li = []
            for url in urls:
                for child in pd.read_html(url): 
                    df = child.T
                    df.columns = df.iloc[1]
                    df = df.iloc[[3]]
                    try:
                        df["tempat lahir"] = [x[0] for x in df["Tempat / Tanggal Lahir"].str.split("/")]
                        df["tanggal lahir"] = [x[1] for x in df["Tempat / Tanggal Lahir"].str.split("/")]
                    except:
                        df["tempat lahir"] = "No Data"
                        df["tanggal lahir"] = "No Data"

                    df = df.rename(columns={"Nama Lengkap" : "nama",
                                        "Alamat" : "alamat"})
                    df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
                    li.append(df)
            df = pd.concat(li, ignore_index=True)

            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Banten/tangerang_selatan.csv", index=False)
                self.success_list.append("tangerang_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tangerang_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tangerang_selatan failed..\n\n")


    def bandung(self):
        try:
            self.get_url(self.dprd_url182)
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bandung failed..\n\n")


    def bandung_barat(self):
        try:
            soup = self.get_url(self.dprd_url183)
            list_link = []
            for span in soup.find_all("p", {"class":"elementor-image-box-title"}):
                list_link.append(span.find("a", href=True)["href"])
            li = []
            for child in tqdm(list_link) :
                try:
                    soup = self.get_url(child)
                    nama = soup.find_all("h2", {"class":"elementor-heading-title elementor-size-default"})[0].text
                    ttl = soup.find_all("p", {"class":"elementor-heading-title elementor-size-default"})[1].text
                    tempat_lahir = ttl.split(",")[0]
                    tanggal_lahir = ttl.split(",")[1]
                    df = pd.DataFrame.from_dict({"nama" : [nama],
                                                "alamat": ["No Data"],
                                                "tempat lahir" : [tempat_lahir],
                                                "tanggal lahir" : [tanggal_lahir]})
                    li.append(df)
                except:
                    continue
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/bandung_barat.csv", index=False)
                self.success_list.append("bandung_barat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bandung_barat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bandung_barat failed..\n\n")


    def bekasi_kabupaten(self):
        try:
            urls = [self.dprd_url184, self.dprd_url185, self.dprd_url186,
                    self.dprd_url187, self.dprd_url188, self.dprd_url189]
            li = []
            for url in urls:
                df = pd.read_html(url)[0]
                df.columns = df.iloc[0]
                df = df.iloc[1:]
                df = df.rename(columns={"NAMA" : "nama"})
                df["alamat"] = "No Data"
                df = df[["nama", "alamat"]]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/bekasi_kabupaten.csv", index=False)
                self.success_list.append("bekasi_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bekasi_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bekasi_kabupaten failed..\n\n")


    def bogor_kabupaten(self):
        try:
            df = pd.read_html(self.dprd_url190)[0]
            df.columns = [0, 1, 2, "nama", 4]
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/bogor_kabupaten.csv", index=False)
                self.success_list.append("bogor_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bogor_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bogor_kabupaten failed..\n\n")


    def ciamis_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url191)
            df = pd.DataFrame.from_dict({"nama" : [x.text.replace("\xa0"," ") for x in soup.find_all("p", {"style":"font-size:22px"})]})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/ciamis_kabupaten.csv", index=False)
                self.success_list.append("ciamis_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("ciamis_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("ciamis_kabupaten failed..\n\n")


    def cianjur_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("cianjur_kabupaten failed..\n\n")


    def cirebon_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url193)
            df = pd.DataFrame.from_dict({"nama" : [x.text.replace("\xa0"," ") for x in soup.find_all("p", {"style":"font-size:22px"})]})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/cirebon_kabupaten.csv", index=False)
                self.success_list.append("cirebon_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("cirebon_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("cirebon_kabupaten failed..\n\n")


    def garut_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("garut_kabupaten failed..\n\n")


    def indramayu_kabupaten(self):
        try:
            df_list = pd.read_html(self.dprd_url195)
            df = pd.concat(df_list, ignore_index=True)
            df = df.rename(columns={"Nama":"nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/indramayu_kabupaten.csv", index=False)
                self.success_list.append("indramayu_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("indramayu_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("indramayu_kabupaten failed..\n\n")


    def karawang_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url196)
            list_nama = [x.find("h3").text for x in soup.find_all("div", {"class":"team-desc"})]
            list_nama = list_nama[1:]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/karawang_kabupaten.csv", index=False)
                self.success_list.append("karawang_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("karawang_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("karawang_kabupaten failed..\n\n")


    def kuningan_kabupaten(self):
        try:
            list_df = pd.read_html(self.dprd_url197)
            df = pd.concat(list_df, ignore_index=True)
            df.columns = [0, "nama", 2,3]
            df = df[df["nama"] != "NAMA"].reset_index(drop=True)
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/kuningan_kabupaten.csv", index=False)
                self.success_list.append("kuningan_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kuningan_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kuningan_kabupaten failed..\n\n")


    def majalengka_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("majalengka_kabupaten failed..\n\n")


    def pangandaran_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("pangandaran_kabupaten failed..\n\n")


    def purwakarta_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("purwakarta_kabupaten failed..\n\n")


    def subang_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("subang_kabupaten failed..\n\n")


    def get_nama_sukabumi(self, inp):
        inp = inp.text
        inp = inp.replace("KETUA ", "")
        inp = inp.replace("WAKIL KETUA  ", "")
        inp = inp.replace("SEKRETARIS  ", "")
        inp = inp.replace("ANGGOTA  ", "")
        return inp


    def sukabumi_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url202)
            df = pd.DataFrame.from_dict({"nama" : [self.get_nama(x) for x in soup.find_all("p", {"style":"text-align: center;"})]})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/sukabumi_kabupaten.csv", index=False)
                self.success_list.append("sukabumi_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sukabumi_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sukabumi_kabupaten failed..\n\n")


    def sumedang_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("sumedang_kabupaten failed..\n\n")


    def tasikmalaya_kabupaten(self):
        try:
            urls = [self.dprd_url204, self.dprd_url204_1, self.dprd_url204_2, 
                    self.dprd_url204_3, self.dprd_url204_4, self.dprd_url204_5]
            list_link=[]
            for url in urls:
                soup = self.get_url(url)
                test_list = []
                spans = soup.find_all("div", {"class":"td-ss-main-content"})
                for span in spans:
                    test_list.append(span.find_all("h3", {"class":"entry-title td-module-title"}))
                    
                for child in test_list[0]:
                    list_link.extend([x["href"] for x in child.find_all("a", href=True)])
            list_link = list_link[3:]
            list_nama = []
            for child in list_link:
                soup = self.get_url(child)
                list_nama.extend([x.text for x in soup.find_all("h1", {"class":"entry-title"})])
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/tasikmalaya_kabupaten.csv", index=False)
                self.success_list.append("tasikmalaya_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tasikmalaya_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tasikmalaya_kabupaten failed..\n\n")


    def bandung_kota(self):
        try:
            soup = self.get_url(self.dprd_url205)
            list_link = []
            spans = soup.find_all('a', attrs={'class':'proling'}, href=True)
            for span in spans:
                list_link.append(span['href'])
            list_nama = []
            list_tempat = []
            list_tanggal = []
            list_alamat = []
            for url in tqdm(list_link):
                # get nama
                list_nama.append(url.split("/")[-1].replace("-", " "))
                soup = self.get_url(url)
                spans = soup.find_all('div', {'class':'a'})
                # spans
                for idx in range(len(spans)):
                    if idx == 0:
                        ttl = str(spans[idx]).replace('<div class="a">', '')
                        ttl = ttl.replace("</span></div>", "")
                        ttl = ttl.replace("<span>", "")
                        ttl = ttl.replace("\xa0", "")

                        tempat = ttl.split("<br/>")[0]
                        tanggal = ttl.split("<br/>")[1]
                        
                        list_tempat.append(tempat)
                        list_tanggal.append(tanggal)

                    elif idx == 1:
                        list_alamat.append(spans[idx].text)
            df = pd.DataFrame.from_dict({"nama" : list_nama,
                             "alamat" : list_alamat,
                             "tempat lahir" : list_tempat,
                             "tanggal lahir" : list_tanggal})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/bandung_kota.csv", index=False)
                self.success_list.append("bandung_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bandung_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bandung_kota failed..\n\n")


    def banjar_kota(self):
        try:
            soup = self.get_url(self.dprd_url206)
            table = soup.find( "table")

            df = pd.read_html(str(table))[0]
            df.columns = [0, 1, 2, "nama", 4, 5, 6]
            df = df.iloc[1:]
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]].reset_index(drop=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/banjar_kota.csv", index=False)
                self.success_list.append("banjar_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("banjar_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("banjar_kota failed..\n\n")


    def bekasi_kota(self):
        try:
            soup = self.get_url(self.dprd_url207)
            spans = soup.find_all("p")
            spans = spans[10:]
            list_nama = []
            for span in spans:
                try:
                    text = (str(span).split(")")[1])
                    text = text.replace("</strong></span><br/>", "")
                    text = text.replace("</p>", "")
                    text = text.split("adalah lembaga perwakilan")[0]
                    list_nama.extend(text.split("<br/>"))
                except:
                    continue
            df = pd.DataFrame.from_dict({"nama" : list_nama[:-2]})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/bekasi_kota.csv", index=False)
                self.success_list.append("bekasi_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bekasi_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bekasi_kota failed..\n\n")


    def bogor_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("bogor_kota failed..\n\n")


    def cimahi_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("cimahi_kota failed..\n\n")


    def cirebon_kota(self):
        try:
            soup = self.get_url(self.dprd_url210)
            nama_cirebon = []
            spans = soup.find_all('p', attrs={'class':'has-text-align-center'})
            for span in spans:
                nama_cirebon.append(span.string)
            df = pd.DataFrame({"nama" : nama_cirebon})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/cirebon_kota.csv", index=False)
                self.success_list.append("cirebon_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("cirebon_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("cirebon_kota failed..\n\n")


    def depok_kota(self):
        try:
            urls = [self.dprd_url211, self.dprd_url212, self.dprd_url213, 
                    self.dprd_url214, self.dprd_url215, self.dprd_url216, self.dprd_url217]
            nama_depok = []
            for url in tqdm(urls):
                soup = self.get_url(url)
                spans = soup.find_all('h6', attrs={'class':'entry-title'})
                for span in spans:
                    nama_depok.append(span.string)
            df = pd.DataFrame({"nama" : nama_depok})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Barat/depok_kota.csv", index=False)
                self.success_list.append("depok_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("depok_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("depok_kota failed..\n\n")


    def sukabumi_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("sukabumi_kota failed..\n\n")


    def tasikmalaya_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("tasikmalaya_kota failed..\n\n")


    def banjarnegara_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("banjarnegara_kabupaten failed..\n\n")


    def banyumas_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url221)
            r= []
            spans = soup.find_all("ol", {"start" : "1"})
            for span in spans:
                text_extract = span.find_all("span")
                r.extend([x.text.replace("\xa0", "") for x in text_extract])
            list_nama = [x.split(" -")[0] for x in r]
            df = pd.DataFrame({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/banyumas_kabupaten.csv", index=False)
                self.success_list.append("banyumas_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("banyumas_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("banyumas_kabupaten failed..\n\n")


    def batang_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("batang_kabupaten failed..\n\n")


    def blora_kabupaten(self):
        try:
            df = pd.read_html(self.get_url(self.dprd_url223))[0]
            df.columns = [0, "nama", 2, 3, 4]
            df = df.iloc[2:]
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]].reset_index(drop=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/blora_kabupaten.csv", index=False)
                self.success_list.append("blora_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("blora_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("blora_kabupaten failed..\n\n")


    def boyolali_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("boyolali_kabupaten failed..\n\n")


    def brebes_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("brebes_kabupaten failed..\n\n")


    def cilacap_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("cilacap_kabupaten failed..\n\n")


    def demak_kabupaten(self):
        try:
            df_list = pd.read_html(self.dprd_url227)
            df = df_list[0]
            df.columns = ["note", "nama", 2, 3]
            df = df[~df["nama"].isnull()].reset_index(drop=True)
            df = df[df["nama"] != "NAMA"].reset_index(drop=True)
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/demak_kabupaten.csv", index=False)
                self.success_list.append("demak_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("demak_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("demak_kabupaten failed..\n\n")


    def grobogan_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url228)
            spans = soup.find_all("div", {"class":"td-module-container td-category-pos-"})
            list_link = []
            for span in spans:
                list_link.append(span.find("a", href=True)["href"])
            li = []
            for child in list_link:
                df = pd.read_html(child)[0].T
                df.columns = df.iloc[0]
                df = df.iloc[[2]]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            df = df.rename(columns={"Nama" : "nama",
                                "Alamat rumah" : "alamat",
                                "Tempat / Tgl lahir" : "TTL"})
            df["tempat lahir"] = [x[0] for x in df["TTL"].str.split(",")]
            df["tanggal lahir"] = [x[1] for x in df["TTL"].str.split(",")]
            df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/grobogan_kabupaten.csv", index=False)
                self.success_list.append("grobogan_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("grobogan_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("grobogan_kabupaten failed..\n\n")


    def jepara_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("jepara_kabupaten failed..\n\n")


    def karanganyar_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("karanganyar_kabupaten failed..\n\n")


    def kebumen_kabupaten(self):
        try:
            df_list = pd.read_html(self.dprd_url231)
            df = pd.concat(df_list, ignore_index=True)
            df.columns = ["No", "nama", "Jabatan"]
            df = df[df["nama"] != "NAMA"].reset_index(drop=True)
            df["alamat"] = "No Data"
            df = df [["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/kebumen_kabupaten.csv", index=False)
                self.success_list.append("kebumen_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kebumen_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kebumen_kabupaten failed..\n\n")


    def kendal_kabupaten(self):
        try:
            df_list = pd.read_html(self.dprd_url232)
            df = pd.concat(df_list, ignore_index=True)
            df.columns = ["No", "nama", "Partai", "Jabatan"]
            df = df[df["nama"] != "NAMA"].reset_index(drop=True)
            df["alamat"] = "No Data"
            df = df [["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/kendal_kabupaten.csv", index=False)
                self.success_list.append("kendal_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kendal_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kendal_kabupaten failed..\n\n")


    def klaten_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url233)
            spans = soup.find_all("p", {"style":"text-align: center;"})
            list_alamat = []
            list_nama = []
            for idx in range(0,len(spans),2):
                alamat = str(spans[idx]).split("<br/>")[1]
                list_alamat.append(alamat.replace("</p>", ""))
                list_nama.append(spans[idx].find("strong").text)                
            df = pd.DataFrame.from_dict({"nama" : list_nama,
                                        "alamat" : list_alamat})
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/klaten_kabupaten.csv", index=False)
                self.success_list.append("klaten_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("klaten_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("klaten_kabupaten failed..\n\n")


    def kudus_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("kudus_kabupaten failed..\n\n")


    def magelang_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("magelang_kabupaten failed..\n\n")


    def pati_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("pati_kabupaten failed..\n\n")


    def pekalongan_kabupaten(self):
        try:
            urls = [self.dprd_url237_1, self.dprd_url237_2, 
                    self.dprd_url237_3, self.dprd_url237_4]
            li=[]
            for url in urls:
                df = pd.read_html(url)[0]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            df = df.rename(columns={"Anggota Dewan" : "nama"})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/pekalongan_kabupaten.csv", index=False)
                self.success_list.append("pekalongan_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pekalongan_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pekalongan_kabupaten failed..\n\n")


    def pemalang_kabupaten(self):
        try:
            url = self.dprd_url238
            df = pd.concat(pd.read_html(url), ignore_index=True)
            df.columns = ["No", "nama", "unsur", "jabatan"]
            df = df[df["nama"] != "Nama"].reset_index(drop=True)
            df = df[df["nama"] != "Komisi A : Bidang Pemerintahan"].reset_index(drop=True)
            df = df[df["nama"] != "Komisi B : Bidang Pembangunan"].reset_index(drop=True)
            df = df[df["nama"] != "Komisi C : Bidang Ekonomi dan Keuangan"].reset_index(drop=True)
            df = df[df["nama"] != "Komisi D : Bidang Kesejahteraan Rakyat"].reset_index(drop=True)
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/pemalang_kabupaten.csv", index=False)
                self.success_list.append("pemalang_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pemalang_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pemalang_kabupaten failed..\n\n")


    def purbalingga_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("purbalingga_kabupaten failed..\n\n")


    def purworejo_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("purworejo_kabupaten failed..\n\n")


    def rembang_kabupaten(self):
        try:
            df = pd.read_html(self.dprd_url241)[0]
            df.columns = ["No", "nama", "partai", "jabatan"]
            df = df.iloc[1:].reset_index(drop=True)
            df['alamat'] = "No Data"
            df = df [['nama', 'alamat']]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/rembang_kabupaten.csv", index=False)
                self.success_list.append("rembang_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("rembang_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("rembang_kabupaten failed..\n\n")


    def semarang_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url242)
            list_nama = [x.text for x in soup.find("ol").find_all("li")]
            df = pd.DataFrame.from_dict({"nama" : [x.split(" (")[0] for x in list_nama]})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/semarang_kabupaten.csv", index=False)
                self.success_list.append("semarang_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("semarang_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("semarang_kabupaten failed..\n\n")


    def sragen_kabupaten(self):
        try:
            df = pd.read_html(self.dprd_url243)[0]
            df.columns = ["no", "nama", "partai"]
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/sragen_kabupaten.csv", index=False)
                self.success_list.append("sragen_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sragen_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sragen_kabupaten failed..\n\n")


    def sukoharjo_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("sukoharjo_kabupaten failed..\n\n")


    def tegal_kabupaten(self):
        try:
            urls = [self.dprd_url245, self.dprd_url246, self.dprd_url247, self.dprd_url248, 
                    self.dprd_url249, self.dprd_url250, self.dprd_url251, self.dprd_url252, 
                    self.dprd_url253, self.dprd_url254, self.dprd_url255, self.dprd_url256, 
                    self.dprd_url257, self.dprd_url258]
            li = []
            for url in tqdm(urls):
                try:
                    df = pd.read_html(url)[0]
                    li.append(df)
                except:
                    continue
            df = pd.concat(li, ignore_index=True)
            df.columns = ["no", "nama", "jabatan"]
            df["alamat"] = "No Data"
            df = df[df["nama"] != "NAMA"].reset_index(drop=True)
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/tegal_kabupaten.csv", index=False)
                self.success_list.append("tegal_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tegal_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tegal_kabupaten failed..\n\n")


    def temanggung_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("temanggung_kabupaten failed..\n\n")


    def wonogiri_kabupaten(self):
        try:
            urls = [self.dprd_url260_1, self.dprd_url260_2, self.dprd_url260_3, 
                    self.dprd_url260_4, self.dprd_url260_5]
            li = []
            for url in urls:
                df_list = pd.read_html(url)
                for child in df_list:
                    df = child.T
                    df.columns = ["nama", "fraksi", "dapil", "email"]
                    df = df.iloc[[2]]
                    df["alamat"] = "No Data"
                    df = df[["nama", "alamat"]]
                    li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/wonogiri_kabupaten.csv", index=False)
                self.success_list.append("wonogiri_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("wonogiri_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("wonogiri_kabupaten failed..\n\n")


    def wonosobo_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("wonosobo_kabupaten failed..\n\n")


    def magelang_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("magelang_kabupaten failed..\n\n")


    def pekalongan_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("pekalongan_kabupaten failed..\n\n")


    def salatiga_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url264)
            list_nama = []
            for span in soup.find_all("ol"):
                list_nama.extend([x.text for x in span.find_all("li")])
            list_nama = [''.join(x.split(".")[:-1]) for x in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/salatiga_kabupaten.csv", index=False)
                self.success_list.append("salatiga_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("salatiga_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("salatiga_kabupaten failed..\n\n")


    def salatiga_kabupaten(self):
        try:
            urls = [self.dprd_url265_1, self.dprd_url265_2, 
                    self.dprd_url265_3, self.dprd_url265_4]
            list_link = []
            for url in urls:
                soup = self.get_url(url)
                for span in soup.find_all("a", {"class":"btn btn-primary"}, href=True):
                    list_link.append(span["href"])
            li = []
            filed_list = []
            for url in tqdm(list_link):
                try:
                    df = pd.read_html(url)[0].T
                    df.columns = ["nama", "TTL", "Jenis Kelamin", "Agama", "alamat", "Komisi", "Fraksi", "Dapil", "Jabatan", "Badan"]
                    df = df.iloc[[2]]
                    df["tempat lahir"] = [x.split(",")[0] for x in df["TTL"]]
                    df["tanggal lahir"] = [x.split(",")[1] for x in df["TTL"]]
                    df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
                    li.append(df)
                except:
                    filed_list.append(url)
                    continue
            for idx in tqdm(range(len(filed_list))):
                df = pd.read_html(filed_list[idx])[0].T
                df.columns = ["nama", "TTL", "Jenis Kelamin", "Agama", "alamat", "Komisi", "Fraksi", "Dapil", "Jabatan", "Badan"]
                df = df.iloc[[2]]
                df["tempat lahir"] = [x.split(",")[0] for x in df["TTL"]]
                df["tanggal lahir"] = [x.split(",")[1] for x in df["TTL"]]
                df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            df = df.drop([x for x in range(47, 56)]).reset_index(drop=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/salatiga_kabupaten.csv", index=False)
                self.success_list.append("salatiga_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("salatiga_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("salatiga_kabupaten failed..\n\n")


    def surakarta_kabupaten(self):
        try:
            urls = [self.dprd_url266, self.dprd_url267, 
                    self.dprd_url268, self.dprd_url269]
            list_link = []
            for url in urls:
                soup = self.get_url(url)
                for span in soup.find_all("ul", "cat-grid grid-col-3 clearfix"):
                    for child in span.find_all("a", href=True):
                        list_link.append(child["href"])
            li = []
            for url in tqdm(list_link):
                soup = self.get_url(url)
                li.append([x.text for x in soup.find_all("td", {"width" : "60%"})])
            df = pd.DataFrame(li)
            df = df.drop_duplicates().reset_index(drop=True)
            df.columns = ["nama", "TTL", "jenis kelamin", "agama", "alamat", "pendidikan", "dapil", "fraksi"]
            df["tempat lahir"] = [x.split(",")[0] for x in df["TTL"]]
            df["tanggal lahir"] = [x.split(",")[1] if len(x.split(",")) == 2 else "" for x in df["TTL"]]
            df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Tengah/surakarta_kabupaten.csv", index=False)
                self.success_list.append("surakarta_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("surakarta_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("surakarta_kabupaten failed..\n\n")


    def tegal_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("tegal_kota failed..\n\n")


    def bangkalan_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("bangkalan_kabupaten failed..\n\n")


    def banyuwangi_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url276)
            list_nama = [x.find("strong").text for x in soup.find_all("div", {"class":"card-body"})]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/banyuwangi_kabupaten.csv", index=False)
                self.success_list.append("banyuwangi_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("banyuwangi_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("banyuwangi_kabupaten failed..\n\n")


    def blitar_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url277)
            list_nama = [y.text for x in soup.find_all("div", {"class" : "post-content"}) for y in x.find_all("strong")]            
            list_nama = list_nama[::2]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/blitar_kabupaten.csv", index=False)
                self.success_list.append("blitar_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("blitar_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("blitar_kabupaten failed..\n\n")


    def bojonegoro_kabupaten(self):
        try:
            urls = [self.dprd_url278, self.dprd_url279, self.dprd_url280, self.dprd_url281]
            li = []
            for url in urls:
                df = pd.read_html(url)[0]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            df.columns = ["no", "nama", "fraksi", "jabatan"]
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/bojonegoro_kabupaten.csv", index=False)
                self.success_list.append("bojonegoro_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bojonegoro_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bojonegoro_kabupaten failed..\n\n")


    def bondowoso_kabupaten(self):
        try:
            urls = [self.dprd_url282, self.dprd_url283, self.dprd_url284, self.dprd_url285, 
                    self.dprd_url286, self.dprd_url287]
            li = []
            for url in urls:
                df = pd.read_html(url)[0]
                df.columns = ["no", "nama", "jabatan", "partai"]
                df = df.iloc[1:]
                df["alamat"] = "No Data"
                df = df[["nama", "alamat"]]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/bondowoso_kabupaten.csv", index=False)
                self.success_list.append("bondowoso_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bondowoso_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bondowoso_kabupaten failed..\n\n")


    def gresik_kabupaten(self):
        try:
            urls = [self.dprd_url288_0, self.dprd_url288, self.dprd_url289, self.dprd_url290,
                    self.dprd_url291, self.dprd_url292, self.dprd_url293, self.dprd_url294]
            list_nama = []
            list_ttl = []
            list_alamat = []

            for url in tqdm(urls):
                soup = self.get_url(url)
                list_nama.extend(list(set([x["data-name"] for x in soup.find_all("a", {"class":"open-profileModal"})])))
                list_ttl.extend(list(set([x["data-birth"] for x in soup.find_all("a", {"class":"open-profileModal"})])))
                list_alamat.extend(list(set([x["data-address"] for x in soup.find_all("a", {"class":"open-profileModal"})])))        
            df = pd.DataFrame.from_dict({"nama" : list_nama,
                            "ttl" : list_ttl,
                            "alamat" : list_alamat})
            df["tempat lahir"] = [x.split(",")[0] if "," in x else x.split(" ")[0] for x in df["ttl"]]
            df["tanggal lahir"] = [x.split(",")[1] if "," in x else x.split(" ")[1] for x in df["ttl"]]
            df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/gresik_kabupaten.csv", index=False)
                self.success_list.append("gresik_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("gresik_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("gresik_kabupaten failed..\n\n")


    def jember_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("jember_kabupaten failed..\n\n")


    def jombang_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("jombang_kabupaten failed..\n\n")


    def kediri_kabupaten(self):
        try:
            df_list = pd.read_html(self.dprd_url297)[:-1]
            li = []
            for df in df_list:
                df.columns = ["no", "nama", "partai", "jabatan"]
                df = df.iloc[1:]
                df["alamat"] = "No Data"
                df = df[["nama", "alamat"]]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/kediri_kabupaten.csv", index=False)
                self.success_list.append("kediri_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kediri_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kediri_kabupaten failed..\n\n")


    def lamongan_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("lamongan_kabupaten failed..\n\n")


    def lumanjang_kabupaten(self):
        try:
            urls = [self.dprd_url299, self.dprd_url300_1, self.dprd_url300_2, 
                    self.dprd_url300_3, self.dprd_url300_4]
            list_link = []
            for url in urls:
                soup = self.get_url(url)
                list_link.extend([y["href"] for x in soup.find_all("div", {"class" : "caption"}) for y in x.find_all("a", href=True)])
            li = []
            for url in tqdm(list_link):
                df = pd.read_html(url)[0].T
                df.columns = ["nama", "ttl", "Jenis Kelamin", "alamat", "Agama", "Fraksi", "Alat Kelengkapan", "Pendidikan", "Organisasi"]
                df = df.iloc[[2]]
                df["tempat lahir"] = [x.split(",")[0] for x in df["ttl"]]
                df["tanggal lahir"] = [x.split(",")[1] for x in df["ttl"]]
                df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/lumanjang_kabupaten.csv", index=False)
                self.success_list.append("lumanjang_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("lumanjang_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("lumanjang_kabupaten failed..\n\n")


    def madiun_kabupaten(self):
        self.error_desc.append("No data")
        self.failed_list.append("madiun_kabupaten failed..\n\n")


    def magetan_kabupaten(self):
        try:
            list_df = pd.read_html(self.dprd_url304)[:-1]
            li = []
            for df in list_df:
                df.columns = df.iloc[0]
                df = df[1:]
                df = df[["NAMA CALON TERPILIH"]]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            df.columns = ["nama"]
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/magetan_kabupaten.csv", index=False)
                self.success_list.append("magetan_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("magetan_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("magetan_kabupaten failed..\n\n")


    def malang_kabupaten(self):
        try:
            df = pd.read_html(self.dprd_url305)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/malang_kabupaten.csv", index=False)
                self.success_list.append("malang_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("malang_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("malang_kabupaten failed..\n\n")


    def mojokerto_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url306)
            df = pd.DataFrame.from_dict({ "nama" : [x.find("h1").text for x in soup.find_all("div", {"id":"anggota-list"})]})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/mojokerto_kabupaten.csv", index=False)
                self.success_list.append("mojokerto_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("mojokerto_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("mojokerto_kabupaten failed..\n\n")


    def nganjuk_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url307)
            df = pd.DataFrame.from_dict({ "nama" : [x.find("h1").text for x in soup.find_all("div", {"id":"anggota-list"})]})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/nganjuk_kabupaten.csv", index=False)
                self.success_list.append("nganjuk_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("nganjuk_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("nganjuk_kabupaten failed..\n\n")


    def ngawi_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("ngawi_kabupaten failed..\n\n")


    def pacitan_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("pacitan_kabupaten failed..\n\n")


    def pamekasan_kabupaten(self):
        try:
            df = pd.read_html(self.dprd_url312)[0]
            df.columns = ["nama", "jabatan", "fraksi", "note"]
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/pamekasan_kabupaten.csv", index=False)
                self.success_list.append("pamekasan_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pamekasan_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pamekasan_kabupaten failed..\n\n")


    def pasuruan_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url313)
            list_nama = [y.text.replace("\n", "") for x in soup.find_all("div", {"class", "wpsm_panel-body"}) for y in x.find_all("li")]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df["nama"] = df["nama"].str.replace("\xa0", "")
            df["nama"] = df["nama"].str.replace("Koordinator : ", "")
            df["nama"] = df["nama"].str.replace(":", "")
            df["nama"] = df["nama"].str.replace("Ketua", "")
            df["nama"] = df["nama"].str.replace("Wakil Ketua", "")
            df["nama"] = df["nama"].str.replace("Sekretaris", "")
            df["nama"] = df["nama"].str.replace("Anggota", "")
            df["nama"] = df["nama"].str.replace("\r", "")
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/pasuruan_kabupaten.csv", index=False)
                self.success_list.append("pasuruan_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pasuruan_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pasuruan_kabupaten failed..\n\n")


    def ponorogo_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url315)
            get_x = [x.find_all("td", {"style":"vertical-align: middle;"}) for x in soup.find_all("table", {"class":"alignleft"})][0]
            list_nama = [y.find("strong").text for y in get_x]
            list_ttl = [z.find("p").text for z in get_x]
            list_alamat = [z.find_all("p")[1].text for z in get_x]
            df = pd.DataFrame.from_dict({"nama":list_nama,
                            "TTL":list_ttl,
                            "alamat":list_alamat})
            df["tempat lahir"] = [x.split(",")[0] for x in df["TTL"]]
            df["tanggal lahir"] = [x.split(",")[1] if "," in x else x.split(".")[1] for x in df["TTL"]]
            df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/ponorogo_kabupaten.csv", index=False)
                self.success_list.append("ponorogo_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("ponorogo_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("ponorogo_kabupaten failed..\n\n")


    def probolinggo_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("probolinggo_kabupaten failed..\n\n")


    def sampang_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url323)
            list_nama = [y.text for x in soup.find_all("ol")[1:] for y in x.find_all("li")]
            list_nama = [x.split("(")[0] for x in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/sampang_kabupaten.csv", index=False)
                self.success_list.append("sampang_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sampang_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sampang_kabupaten failed..\n\n")


    def sidoarjo_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url324)
            list_nama = [x.text for x in soup.find_all("p")[4:13]]
            list_nama = ",".join(list_nama)
            list_nama = list_nama.replace("1. PKB: ", "")
            list_nama = list_nama.replace("2. PDIP: ", "")
            list_nama = list_nama.replace("3. Gerindra: ", "")
            list_nama = list_nama.replace("4. PAN:", "")
            list_nama = list_nama.replace("5. Golkar:", "")
            list_nama = list_nama.replace("6. PKS:", "")
            list_nama = list_nama.replace("7. NasDem: ", "")
            list_nama = list_nama.replace("8. Demokrat: ", "")
            list_nama = list_nama.replace("9. PPP: ", "")
            list_nama = list_nama.replace(" (*)", "")
            list_nama = list_nama.replace(";", "")
            list_nama = list_nama.split(",")
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "no data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/sidoarjo_kabupaten.csv", index=False)
                self.success_list.append("sidoarjo_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sidoarjo_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sidoarjo_kabupaten failed..\n\n")


    def situbondo_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url325)
            list_nama = [x.text.split(" - ")[0] for x in soup.find_all("p")[6:63]]
            list_nama.remove('Dapil I\xa0')
            list_nama.remove('Dapil II\xa0')
            list_nama.remove('Dapil III')
            list_nama.remove('Dapil IV')
            list_nama.remove('Dapil V')
            list_nama.remove('Dapil VI')
            list_nama = "-I-".join(list_nama)
            list_nama = list_nama.replace('\xa0', '')
            list_nama = list_nama.replace('1. ', '')
            list_nama = list_nama.replace('2. ', '')
            list_nama = list_nama.replace('3. ', '')
            list_nama = list_nama.replace('4. ', '')
            list_nama = list_nama.replace('5. ', '')
            list_nama = list_nama.replace('6.', '')
            list_nama = list_nama.replace('7. ', '')
            list_nama = list_nama.replace('8. ', '')
            list_nama = list_nama.replace('9. ', '')
            list_nama = list_nama.replace('10. ', '')
            list_nama = list_nama.split("-I-")
            list_nama = list(filter(None, list_nama))
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/situbondo_kabupaten.csv", index=False)
                self.success_list.append("situbondo_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("situbondo_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("situbondo_kabupaten failed..\n\n")


    def sumenep_kabupaten(self):
        try:
            urls = [self.dprd_url326, self.dprd_url327, 
                    self.dprd_url328, self.dprd_url329]
            list_nama = []
            for url in urls:
                soup = self.get_url(url)
                list_nama.extend([x.find("h4").text for x in soup.find_all("div", {"class":"partner-block"})])
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/sumenep_kabupaten.csv", index=False)
                self.success_list.append("sumenep_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sumenep_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sumenep_kabupaten failed..\n\n")


    def trenggalek_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url331)
            list_link = [self.dprd_url330+y["href"] for x in soup.find_all("table", {"class":"category"}) for y in x.find_all("a", href=True)][4:-8]
            list_nama = []
            list_ttl = []
            list_alamat = []

            for url in tqdm(list_link):
                soup = self.get_url(url)
                list_nama.append([x.text for x in soup.find_all("span", {"style":"mso-no-proof: yes;"})][0])
                list_ttl.append([x.text for x in soup.find_all("span", {"style":"mso-no-proof: yes;"})][1])
                list_alamat.append([x.text for x in soup.find_all("span", {"style":"mso-no-proof: yes;"})][5])
            df = pd.DataFrame.from_dict({"nama":list_nama,
                            "TTL":list_ttl,
                            "alamat":list_alamat})
            df["tempat lahir"] = [x.split(",")[0] for x in df["TTL"]]
            df["tanggal lahir"] = [x.split(",")[1] for x in df["TTL"]]
            df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]              
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/trenggalek_kabupaten.csv", index=False)
                self.success_list.append("trenggalek_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("trenggalek_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("trenggalek_kabupaten failed..\n\n")


    def tuban_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url332)
            list_link = [y["href"] for x in soup.find_all("table") for y in x.find_all('a', href=True)]
            li = []
            for url in list_link:
                df = pd.read_html(url)[0].T
                df.columns = df.iloc[0]
                df = df.iloc[[2]]
                df = df [["Nama Lengkap", "Tempat/Tgl. Lahir", "Alamat Lengkap/No.Telp"]]
                df.columns = ["nama", "ttl", "alamat"]
                df["tempat lahir"] = [x.split(",")[0] for x in df["ttl"]]
                df["tanggal lahir"] = [x.split(",")[1] for x in df["ttl"]]
                df = df [["nama", "alamat", "tempat lahir", "tanggal lahir"]]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/tuban_kabupaten.csv", index=False)
                self.success_list.append("tuban_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tuban_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tuban_kabupaten failed..\n\n")


    def tulungagung_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url334)
            list_link = list(set([y["href"] for x in soup.find_all("div", {"class", "td-block-span4"}) for y in x.find_all('a', href=True)][12:]))
            list_link = [k for k in list_link if 'partai' not in  k]
            li = []
            for url in tqdm(list_link):
                try:
                    df = pd.read_html(url)[0].T
                    df.columns = df.iloc[0]
                    df = df.iloc[[2]]
                    df["nama"] = url.split("/")[-2].replace("-", " ")
                    df["alamat"] = df["Alamat tempat tinggal"] + df["Desa / kelurahan"] + df["Kecamatan"] + df["Kabupaten / Kota"]
                    df["tempat lahir"] = [x.split(",")[0] for x in df["Tempat Tanggal Lahir"]]
                    df["tanggal lahir"] = [x.split(",")[1] for x in df["Tempat Tanggal Lahir"]]
                    df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
                    li.append(df)
                except:
                    df["nama"] = url.split("/")[-2].replace("-", " ")
                    df["alamat"] = "No Data"
                    df["tempat lahir"] = "No Data"
                    df["tanggal lahir"] = "No Data"
                    li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Jawa Timur/tulungagung_kabupaten.csv", index=False)
                self.success_list.append("tulungagung_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tulungagung_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tulungagung_kabupaten failed..\n\n")


    def batu_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("batu_kota failed..\n\n")


    def blitar_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("blitar_kota failed..\n\n")


    def kediri_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("kediri_kota failed..\n\n")


    def madiun_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("madiun_kota failed..\n\n")


    def malang_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("malang_kota failed..\n\n")


    def mojokerto_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("mojokerto_kota failed..\n\n")


    def pasuruan_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("pasuruan_kota failed..\n\n")


    def probolinggo_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("probolinggo_kota failed..\n\n")


    def surabaya_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("surabaya_kota failed..\n\n")


    def jakarta_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("jakarta_barat failed..\n\n")


    def jakarta_pusat(self):
        self.error_desc.append("no data")
        self.failed_list.append("jakarta_pusat failed..\n\n")


    def jakarta_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("jakarta_selatan failed..\n\n")


    def jakarta_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("jakarta_timur failed..\n\n")


    def jakarta_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("jakarta_utara failed..\n\n")


    def kep_seribu(self):
        self.error_desc.append("no data")
        self.failed_list.append("kep_seribu failed..\n\n")


    def bantul(self):
        try:
            urls = [self.dprd_url350, self.dprd_url351, 
                    self.dprd_url352, self.dprd_url353]
            list_nama = []
            # pimpinan
            soup = self.get_url(self.dprd_url350)
            get_nama = [x.find_all("p")[2].text.split("\xa0\xa0") for x in soup.find_all("div", {"class":"body-text"})][0]
            idx_list = [1,2,4,9]
            get_nama = [get_nama[idx] for idx in idx_list]
            list_nama.extend(get_nama)

            # Komisi
            soup = self.get_url(self.dprd_url351)
            body_txt = soup.find("div", {"class":"body-text"})
            text = [x.replace("\xa0", "") for x in body_txt.text.split("\n")[21:-4]]
            text = [x.replace("Ketua :", "") for x in text]
            text = [x.replace("Wakil", "") for x in text]
            text = [x.replace("Sekretaris :", "") for x in text]
            text = [x.split("(")[0] for x in text]
            idx_list = [0,1,2,6,7,8,9,10,11,18,19,20,23,24,25,26,27,28,29,36,37,38,41,42,43,44,45,46,47,48,55,56,57,60,61,62,63,64,65,66,67]
            get_nama = [text[idx] for idx in idx_list]
            list_nama.extend(get_nama)

            # Badan Musyawarah
            df = pd.read_html(self.dprd_url352)[0]
            df.columns = ["no", "nama", "jabatan", "partai"]
            df = df[1:]
            list_nama.extend(df["nama"].to_list())

            # Badan Pembentukan perda
            df = pd.read_html(self.dprd_url353)[0]
            df.columns = ["no", "nama", "jabatan", "partai"]
            df = df[1:]
            list_nama.extend(df["nama"].to_list())

            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Yogyakarta/bantul.csv", index=False)
                self.success_list.append("bantul success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bantul failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bantul failed..\n\n")


    def gunungkidul(self):
        self.error_desc.append("no data")
        self.failed_list.append("gunungkidul failed..\n\n")


    def kulonprogo(self):
        try:
            df = pd.read_html(self.dprd_url356)[0]
            df.dropna(inplace=True)
            arr = df.to_numpy().flatten()
            df = list(arr)
            df = pd.DataFrame.from_dict({"nama":df})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Yogyakarta/kulonprogo.csv", index=False)
                self.success_list.append("kulonprogo success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kulonprogo failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kulonprogo failed..\n\n")


    def sleman(self):
        self.error_desc.append("no data")
        self.failed_list.append("sleman failed..\n\n")


    def yogyakarta_kota(self):
        try:
            df = pd.read_html(self.dprd_url358)[0]
            df.columns = ["no", "ket", "filed", "titik", "nama"]
            df = df[df["filed"] == "NAMA"].reset_index(drop=True)
            df["alamat"]= "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Yogyakarta/yogyakarta_kota.csv", index=False)
                self.success_list.append("yogyakarta_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("yogyakarta_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("yogyakarta_kota failed..\n\n")


    def badung(self):
        try:
            urls = [self.dprd_url359, self.dprd_url360]
            list_nama = []
            list_ttl = []
            list_alamat = []

            for url in urls:
                list_df = pd.read_html(url)
                for idx in range(len(list_df)):
                    df = list_df[idx].iloc[[0]]
                    df.columns = ["no", "ket"]
                    df["ket"] = df["ket"].str.lower()
                    list_nama.append(df["ket"].str.split(":")[0][1])
                    list_ttl.append(df["ket"].str.split(":")[0][2])
                    list_alamat.append(df["ket"].str.split(":")[0][3])
            df = pd.DataFrame.from_dict({"nama":list_nama, 
                             "ttl" : list_ttl, 
                             "alamat":list_alamat})
            df["alamat"][0] = df["ttl"][0].split(",")[2]
            df["ttl"][0] = ",".join(df["ttl"][0].split(",")[:2])
            df["ttl"] = [x.split("alamat")[0] for x in df["ttl"]]
            df["nama"] = [x.split("tempat")[0] for x in df["nama"]] 
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Bali/badung.csv", index=False)
                self.success_list.append("badung success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("badung failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("badung failed..\n\n")


    def bangli(self):
        self.error_desc.append("no data")
        self.failed_list.append("bangli failed..\n\n")


    def buleleng_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("buleleng_kabupaten failed..\n\n")


    def gianyar_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("gianyar_kabupaten failed..\n\n")


    def jembrana(self):
        try:
            soup = self.get_url(self.dprd_url364)
            idx_list = [2,3,4,9,10,11,18,19,20]   
            list_nama = [soup.find_all("p")[idx].text.replace("\xa0", "") for idx in idx_list]
            list_nama.extend([y.text for x in soup.find_all("ol") for y in x.find_all("li")])
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            df["nama"] = df["nama"].str.replace("1. KETUA : ", "")
            df["nama"] = df["nama"].str.replace("2. WAKIL KETUA", "")
            df["nama"] = df["nama"].str.replace(":", "")
            df["nama"] = df["nama"].str.replace("3. SEKRETARIS : ", "")
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Bali/jembrana.csv", index=False)
                self.success_list.append("jembrana success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("jembrana failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("jembrana failed..\n\n")


    def karangasem_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("karangasem_kabupaten failed..\n\n")


    def klungkung_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("klungkung_kabupaten failed..\n\n")


    def tabanan_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("tabanan_kabupaten failed..\n\n")


    def denpasar_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("denpasar_kabupaten failed..\n\n")


    def bima_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("bima_kabupaten failed..\n\n")


    def dompu_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("dompu_kabupaten failed..\n\n")


    def lombok_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("lombok_barat failed..\n\n")


    def lombok_tengah(self):
        self.error_desc.append("no data")
        self.failed_list.append("lombok_tengah failed..\n\n")


    def lombok_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("lombok_timur failed..\n\n")


    def lombok_utara(self):
        try:
            df = pd.read_html(self.dprd_url375)[0]
            df.columns = ["no", "nama", "jabatan", "fraksi"]
            df = df.iloc[2:]
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]].reset_index(drop=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/NTB/lombok_utara.csv", index=False)
                self.success_list.append("lombok_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("lombok_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("lombok_utara failed..\n\n")


    def sumbawa(self):
        self.error_desc.append("no data")
        self.failed_list.append("sumbawa failed..\n\n")


    def sumbawa_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("sumbawa_barat failed..\n\n")


    def bima(self):
        self.error_desc.append("no data")
        self.failed_list.append("bima failed..\n\n")


    def mataram(self):
        self.error_desc.append("no data")
        self.failed_list.append("mataram failed..\n\n")


    def alor(self):
        self.error_desc.append("no data")
        self.failed_list.append("alor failed..\n\n")


    def belu(self):
        self.error_desc.append("no data")
        self.failed_list.append("belu failed..\n\n")


    def ende(self):
        self.error_desc.append("no data")
        self.failed_list.append("ende failed..\n\n")


    def flores_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("flores_timur failed..\n\n")


    def kupang(self):
        self.error_desc.append("no data")
        self.failed_list.append("kupang failed..\n\n")


    def lembata(self):
        self.error_desc.append("no data")
        self.failed_list.append("lembata failed..\n\n")


    def malaka(self):
        self.error_desc.append("no data")
        self.failed_list.append("malaka failed..\n\n")


    def manggarai(self):
        self.error_desc.append("no data")
        self.failed_list.append("manggarai failed..\n\n")


    def manggarai_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("manggarai_barat failed..\n\n")


    def manggarai_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("manggarai_timur failed..\n\n")


    def ngada(self):
        self.error_desc.append("no data")
        self.failed_list.append("ngada failed..\n\n")


    def nagekeo(self):
        self.error_desc.append("no data")
        self.failed_list.append("nagekeo failed..\n\n")


    def rote_ndao(self):
        self.error_desc.append("no data")
        self.failed_list.append("rote_ndao failed..\n\n")


    def sabu_raijua(self):
        self.error_desc.append("no data")
        self.failed_list.append("sabu_raijua failed..\n\n")


    def sikka(self):
        self.error_desc.append("no data")
        self.failed_list.append("sikka failed..\n\n")


    def sumba_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("sumba_barat failed..\n\n")


    def sumba_barat_daya(self):
        self.error_desc.append("no data")
        self.failed_list.append("sumba_barat_daya failed..\n\n")


    def sumba_tengah(self):
        self.error_desc.append("no data")
        self.failed_list.append("sumba_tengah failed..\n\n")


    def sumba_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("sumba_timur failed..\n\n")


    def timor_tengah_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("timor_tengah_selatan failed..\n\n")


    def timor_tengah_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("timor_tengah_utara failed..\n\n")


    def kupang_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("kupang_kota failed..\n\n")


    def bengkayang(self):
        self.error_desc.append("no data")
        self.failed_list.append("bengkayang failed..\n\n")


    def kapuas_hulu(self):
        self.error_desc.append("no data")
        self.failed_list.append("kapuas_hulu failed..\n\n")


    def kayong_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("kayong_utara failed..\n\n")


    def ketapang(self):
        self.error_desc.append("no data")
        self.failed_list.append("ketapang failed..\n\n")


    def kuburaya(self):
        self.error_desc.append("no data")
        self.failed_list.append("kuburaya failed..\n\n")


    def landak(self):
        self.error_desc.append("no data")
        self.failed_list.append("landak failed..\n\n")


    def melawi(self):
        self.error_desc.append("no data")
        self.failed_list.append("melawi failed..\n\n")


    def mempawah(self):
        self.error_desc.append("no data")
        self.failed_list.append("mempawah failed..\n\n")


    def sambas(self):
        self.error_desc.append("no data")
        self.failed_list.append("sambas failed..\n\n")


    def sanggau(self):
        self.error_desc.append("no data")
        self.failed_list.append("sanggau failed..\n\n")


    def sekadau(self):
        self.error_desc.append("no data")
        self.failed_list.append("sekadau failed..\n\n")


    def sintang(self):
        self.error_desc.append("no data")
        self.failed_list.append("sintang failed..\n\n")


    def pontianak(self):
        self.error_desc.append("no data")
        self.failed_list.append("pontianak failed..\n\n")


    def singkawang(self):
        self.error_desc.append("no data")
        self.failed_list.append("singkawang failed..\n\n")


    def balangan(self):
        self.error_desc.append("no data")
        self.failed_list.append("balangan failed..\n\n")


    def banjar(self):
        self.error_desc.append("no data")
        self.failed_list.append("banjar failed..\n\n")


    def barito_kuala(self):
        self.error_desc.append("no data")
        self.failed_list.append("barito_kuala failed..\n\n")


    def hulu_sungai_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("hulu_sungai_selatan failed..\n\n")


    def hulu_sungai_tengah(self):
        self.error_desc.append("no data")
        self.failed_list.append("hulu_sungai_tengah failed..\n\n")


    def hulu_sungai_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("hulu_sungai_utara failed..\n\n")


    def kotabaru(self):
        self.error_desc.append("no data")
        self.failed_list.append("kotabaru failed..\n\n")


    def tabalong(self):
        self.error_desc.append("no data")
        self.failed_list.append("tabalong failed..\n\n")


    def tanah_bumbu(self):
        self.error_desc.append("no data")
        self.failed_list.append("tanah_bumbu failed..\n\n")


    def tanah_laut(self):
        self.error_desc.append("no data")
        self.failed_list.append("tanah_laut failed..\n\n")


    def tapin(self):
        self.error_desc.append("no data")
        self.failed_list.append("tapin failed..\n\n")


    def banjarbaru(self):
        self.error_desc.append("no data")
        self.failed_list.append("banjarbaru failed..\n\n")


    def banjarmasin(self):
        try:
            urls = [self.dprd_url428, self.dprd_url429, self.dprd_url430, self.dprd_url431,
                    self.dprd_url432, self.dprd_url433, self.dprd_url434, self.dprd_url435]
            li = []
            for url in urls:
                df_list = pd.read_html(url)
                for idx in tqdm(range(len(df_list))):
                    df = df_list[idx].T
                    df.columns = ["nama", "ttl", "jabatan"]
                    df = df.iloc[[1]]
                    li.append(df)
            df = pd.concat(li, ignore_index=True)
            df["nama"] = [x.split(":")[1] for x in df["nama"]]
            df["ttl"] = [x.split(":")[1] for x in df["ttl"]]
            df["tempat lahir"] = [x.split(",")[0] for x in df["ttl"]]
            df["tanggal lahir"] = [x.split(":")[1] if len(x.split(":")) == 2 else "No Data" for x in df["ttl"]]
            df["alamat"] = "No Data"
            df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Kalimantan Selatan/banjarmasin.csv", index=False)
                self.success_list.append("banjarmasin success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("banjarmasin failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("banjarmasin failed..\n\n")


    def barito_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("barito_selatan failed..\n\n")


    def barito_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("barito_timur failed..\n\n")


    def barito_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("barito_utara failed..\n\n")


    def gunung_mas(self):
        self.error_desc.append("no data")
        self.failed_list.append("gunung_mas failed..\n\n")


    def kapuas(self):
        self.error_desc.append("no data")
        self.failed_list.append("kapuas failed..\n\n")


    def katingan(self):
        self.error_desc.append("no data")
        self.failed_list.append("katingan failed..\n\n")


    def kotawaringin_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("kotawaringin_barat failed..\n\n")


    def kotawaringin_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("kotawaringin_timur failed..\n\n")


    def lamandau(self):
        self.error_desc.append("no data")
        self.failed_list.append("lamandau failed..\n\n")


    def murungraya(self):
        self.error_desc.append("no data")
        self.failed_list.append("murungraya failed..\n\n")

    def pulaupisau(self):
        self.error_desc.append("no data")
        self.failed_list.append("pulaupisau failed..\n\n")


    def sukamara(self):
        self.error_desc.append("no data")
        self.failed_list.append("sukamara failed..\n\n")


    def seruyan(self):
        self.error_desc.append("no data")
        self.failed_list.append("seruyan failed..\n\n")


    def palangkaraya(self):
        self.error_desc.append("no data")
        self.failed_list.append("palangkaraya failed..\n\n")


    def murungraya(self):
        self.error_desc.append("no data")
        self.failed_list.append("murungraya failed..\n\n")


    def berau(self):
        self.error_desc.append("no data")
        self.failed_list.append("berau failed..\n\n")


    def kutai_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("kutai_barat failed..\n\n")


    def kutai_kartanegara(self):
        self.error_desc.append("no data")
        self.failed_list.append("kutai_kartanegara failed..\n\n")


    def kutai_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("kutai_timur failed..\n\n")


    def mahakam_ulu(self):
        self.error_desc.append("no data")
        self.failed_list.append("mahakam_ulu failed..\n\n")


    def paser(self):
        self.error_desc.append("no data")
        self.failed_list.append("paser failed..\n\n")


    def penajam_paser_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("penajam_paser_utara failed..\n\n")


    def balikpapan_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("balikpapan_kota failed..\n\n")


    def bontang_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("bontang_kota failed..\n\n")


    def samarinda_kota(self):
        try:
            urls = [self.dprd_url459, self.dprd_url460, self.dprd_url461, self.dprd_url462, 
                    self.dprd_url463, self.dprd_url464, self.dprd_url465, self.dprd_url466]
            list_link = []
            for url in urls:
                soup = self.get_url(url)
                list_link.extend([y["href"] for x in soup.find_all("div", {"class":"yellow struktur"}) for y in x.find_all("a", href=True)])
            li = []
            for url in tqdm(list_link):
                soup = self.get_url(url)
                nama_string = soup.find("h2", {"style":"margin-bottom: 20px"}).text

                df = pd.read_html(url)[0].T
                df.columns = ["ttl", "agama", "istri", "alamat"]
                df = df.iloc[[2]]
                df["nama"] = nama_string
                df["tempat lahir"] = [x.split(",")[0] for x in df["ttl"]]
                df["tanggal lahir"] = [x.split(",")[1] if len(x.split(",")) >=2 else "No Data" for x in df["ttl"]]
                df = df[["nama", "alamat", "tempat lahir", "tanggal lahir"]]
                li.append(df)
            df = pd.concat(li, ignore_index=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Kalimantan Timur/samarinda_kota.csv", index=False)
                self.success_list.append("samarinda_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("samarinda_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("samarinda_kota failed..\n\n")


    def bulungan(self):
        self.error_desc.append("no data")
        self.failed_list.append("bulungan failed..\n\n")


    def malinau(self):
        self.error_desc.append("no data")
        self.failed_list.append("malinau failed..\n\n")


    def nunukan(self):
        self.error_desc.append("no data")
        self.failed_list.append("nunukan failed..\n\n")


    def tana_tidung(self):
        self.error_desc.append("no data")
        self.failed_list.append("tana_tidung failed..\n\n")


    def tarakan(self):
        self.error_desc.append("no data")
        self.failed_list.append("tarakan failed..\n\n")


    def boalemo(self):
        self.error_desc.append("no data")
        self.failed_list.append("boalemo failed..\n\n")


    def bone_bolango(self):
        self.error_desc.append("no data")
        self.failed_list.append("boalemo failed..\n\n")


    def gorontalo_kabupaten(self):
        self.error_desc.append("no data")
        self.failed_list.append("gorontalo_kabupaten failed..\n\n")


    def gorontalo_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("gorontalo_utara failed..\n\n")


    def pohuwato(self):
        self.error_desc.append("no data")
        self.failed_list.append("pohuwato failed..\n\n")


    def gorontalo_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("gorontalo_kota failed..\n\n")


    def bantaeng(self):
        try:
            soup = self.get_url(self.dprd_url483)
            span = soup.find("div", {"class":"detail_post entry-content"})
            list_nama = [x.text for x in span.find_all("p")[11:]]
            list_nama = [x.split("(")[0] for x in list_nama]
            list_nama = [x.split(".")[1] if len(x.split(".")) >= 2 else "No Data" for x in list_nama]
            list_nama = list(filter(lambda a: a != "No Data", list_nama))
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"]="No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/bantaeng.csv", index=False)
                self.success_list.append("bantaeng success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bantaeng failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bantaeng failed..\n\n")


    def barru_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url484)
            list_nama = [y.text.split("(")[0] for x in soup.find("div", {"class":"entry-content clearfix"}).find_all("ol") for y in x.find_all("li")]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"]="No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/barru.csv", index=False)
                self.success_list.append("barru success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("barru failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("barru failed..\n\n")


    def bone_kabupaten(self):
        try:
            soup = self.get_url(self.dprd_url485)
            spans = [x.text for x in soup.find("div", {"class":"entry-content entry-content-single"}).find_all("p")[9:]]
            spans = [k for k in spans if '(' in k]
            spans = [x.split("\n") for x in spans]
            spans = [j for sub in spans for j in sub]
            spans = [x.split("(")[0] for x in spans]
            list_nama = [" ".join(x.split(".")[1:]) if len(x.split(".")) >= 3 else x.split(".")[1] for x in spans]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/bone_kabupaten.csv", index=False)
                self.success_list.append("bone_kabupaten success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("bone_kabupaten failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("bone_kabupaten failed..\n\n")


    def bulukumba(self):
        self.error_desc.append("no data")
        self.failed_list.append("bulukumba failed..\n\n")


    def enrekang(self):
        self.error_desc.append("no data")
        self.failed_list.append("enrekang failed..\n\n")


    def gowa(self):
        try:
            soup = self.get_url(self.dprd_url488)
            spans = [x.text for x in soup.find("div", {"class":"entry-content entry-content-single"}).find_all("p")[3:]]
            spans = [k for k in spans if '(' in k]
            spans = [x.split("\n") for x in spans]
            spans = [j for sub in spans for j in sub]
            spans = [x.split("(")[0] for x in spans]
            list_nama = [k for k in spans if '*' in k]
            list_nama = [x.split("* ")[1] for x in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/gowa.csv", index=False)
                self.success_list.append("gowa success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("gowa failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("gowa failed..\n\n")


    def jeneponto(self):
        try:
            soup = self.get_url(self.dprd_url489)
            list_nama = [y.text for x in soup.find_all("h6") for y in x.find_all("span", {"style":"font-family: "})]
            list_nama = [k for k in list_nama if '(' in k]
            list_nama = [x.split("(")[0] for x in list_nama]
            list_nama = [" ".join(x.split(".")[1:]) if len(x.split(".")) > 2 else x.split(".")[1] for x in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/jeneponto.csv", index=False)
                self.success_list.append("jeneponto success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("jeneponto failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("jeneponto failed..\n\n")


    def kep_selayar(self):
        try:
            soup = self.get_url(self.dprd_url490)
            list_nama = [x.text for x in soup.find_all("ol")]
            list_nama = [x.split("\n")[1:-1] for x in list_nama][:-1]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [x.split("dari")[0] for x in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/kep_selayar.csv", index=False)
                self.success_list.append("kep_selayar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kep_selayar failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kep_selayar failed..\n\n")


    def luwu(self):
        try:
            soup = self.get_url(self.dprd_url491)
            list_nama = [y.text for x in soup.find_all("div", {"class":"side-article txt-article multi-fontsize"}) for y in x.find_all("p")]
            list_nama = [k for k in list_nama if 'Dapil' in k]
            list_nama = [x.split(":")[1] for x in list_nama]
            list_nama = [x.split(")") for x in list_nama]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            list_nama = [k.split(". ")[1] for k in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/luwu.csv", index=False)
                self.success_list.append("luwu success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("luwu failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("luwu failed..\n\n")


    def luwu_timur(self):
        try:
            soup = self.get_url(self.dprd_url492)
            list_nama = soup.find("div", {"class":"post-content clearfix font17"}).text.split("\n\r\n")
            list_nama = [x.lower() for x in list_nama]
            list_nama = [k for k in list_nama if "daerah pemilihan" in k]
            list_nama = [x.split(":")[-1] for x in list_nama]
            list_nama = [x.split(",") for x in list_nama]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [x.split("dan") for x in list_nama]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [x.split("(")[0] for x in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/luwu_timur.csv", index=False)
                self.success_list.append("luwu_timur success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("luwu_timur failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("luwu_timur failed..\n\n")


    def luwu_utara(self):
        try:
            soup = self.get_url(self.dprd_url493)
            list_nama = [x.text.split("suara") for x in soup.find_all("ol")]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/luwu_utara.csv", index=False)
                self.success_list.append("luwu_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("luwu_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("luwu_utara failed..\n\n")


    def pangkajene_dan_kepulauan(self):
        try:
            urls = [self.dprd_url495, self.dprd_url496, self.dprd_url497, self.dprd_url498]
            list_nama = []
            for url in urls:
                soup = self.get_url(url)
                get_nama = [x.text for x in soup.find_all("h4", {"class":"pp-tm-name"})]
                list_nama.extend(get_nama)
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/pangkajene_dan_kepulauan.csv", index=False)
                self.success_list.append("pangkajene_dan_kepulauan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pangkajene_dan_kepulauan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pangkajene_dan_kepulauan failed..\n\n")


    def pinrang(self):
        try:
            soup = self.get_url(self.dprd_url500)
            list_nama = [y.text.split(" (")[0] for x in soup.find_all("ol") for y in x.find_all("li")]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/pinrang.csv", index=False)
                self.success_list.append("pinrang success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("pinrang failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("pinrang failed..\n\n")


    def sidenreng_rappang(self):
        try:
            soup = self.get_url(self.dprd_url501)
            list_nama = [y.text.split(" (")[0] for x in soup.find_all("ol") for y in x.find_all("li")]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/sidenreng_rappang.csv", index=False)
                self.success_list.append("sidenreng_rappang success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sidenreng_rappang failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sidenreng_rappang failed..\n\n")


    def sinjai(self):
        try:
            soup = self.get_url(self.dprd_url502)
            list_nama = [x.text.split("\n") for x in soup.find_all("p")[11:18]]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/sinjai.csv", index=False)
                self.success_list.append("sinjai success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("sinjai failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("sinjai failed..\n\n")


    def soppeng(self):
        try:
            soup = self.get_url(self.dprd_url503)
            list_nama = [x.text.split("\n") for x in soup.find_all("p")[10:15]]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            list_nama = [' '.join(x.split(".")[1:]) if len(x.split(".")) >= 3 else x.split(".")[1] for x in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/soppeng.csv", index=False)
                self.success_list.append("soppeng success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("soppeng failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("soppeng failed..\n\n")


    def takalar(self):
        try:
            soup = self.get_url(self.dprd_url504)
            list_nama = [x.text.split("\n") for x in soup.find_all("p")[3:6]]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            list_nama = [' '.join(x.split(".")[1:]) if len(x.split(".")) >= 3 else x.split(".")[1] for x in list_nama if '.' in x]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/takalar.csv", index=False)
                self.success_list.append("takalar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("takalar failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("takalar failed..\n\n")


    def tana_taroja(self):
        try:
            soup = self.get_url(self.dprd_url505)
            list_nama = [y.text.split("(")[0] for x in soup.find_all("ol") for y in x.find_all("li")]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/tana_taroja.csv", index=False)
                self.success_list.append("tana_taroja success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tana_taroja failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tana_taroja failed..\n\n")


    def taroja_utara(self):
        try:
            soup = self.get_url(self.dprd_url506)
            list_nama = [y.text for x in soup.find_all("ol") for y in x.find_all("li")]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/taroja_utara.csv", index=False)
                self.success_list.append("taroja_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("taroja_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("taroja_utara failed..\n\n")


    def wajo(self):
        try:
            soup = self.get_url(self.dprd_url507)
            list_nama = [y.text.split(")") for x in soup.find_all("ol") for y in x.find_all("li")]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            list_nama = [k.split(". ")[1] if ". " in k else k for k in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/wajo.csv", index=False)
                self.success_list.append("wajo success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("wajo failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("wajo failed..\n\n")


    def makasar(self):
        try:
            urls = [self.dprd_url508_1, self.dprd_url508_2]
            list_nama = []
            for url in urls:
                soup = self.get_url(url)
                spans = [x.text.split(")") for x in soup.find_all("p")]
                list_nama.extend(spans)
            list_nama = [j for sub in list_nama for j in sub][3:]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            list_nama = [k for k in list_nama if 'Dapil' not in k]
            list_nama = [k.split(". ")[-1] if ". " in k else k for k in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/makasar.csv", index=False)
                self.success_list.append("makasar success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("makasar failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("makasar failed..\n\n")


    def palopo_kota(self):
        try:
            df = pd.read_html(self.dprd_url509)[0]
            df.columns = ["no", "nama", "jabatan", "partai"]
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]].iloc[1:].reset_index(drop=True)
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/palopo_kota.csv", index=False)
                self.success_list.append("palopo_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("palopo_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("palopo_kota failed..\n\n")


    def parepare_kota(self):
        try:
            df = pd.read_html(self.dprd_url510)[0]
            df.columns = ["no", "nama", "fraksi", "partai", "alamat", "no hp"]
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Selatan/parepare_kota.csv", index=False)
                self.success_list.append("parepare_kota success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("parepare_kota failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("parepare_kota failed..\n\n")


    def bombana(self):
        self.error_desc.append("no data")
        self.failed_list.append("bombana failed..\n\n")


    def buton(self):
        self.error_desc.append("no data")
        self.failed_list.append("buton failed..\n\n")


    def buton_selatan(self):
        try:
            soup = pd.get_url(self.dprd_url513)
            list_nama = [x.text for x in soup.find_all("div", {"dir":"auto"})]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/buton_selatan.csv", index=False)
                self.success_list.append("buton_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("buton_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("buton_selatan failed..\n\n")


    def buton_tengah(self):
        try:
            soup = pd.get_url(self.dprd_url514)
            list_nama = [y.text.split(")") for x in soup.find_all("ol") for y in x.find_all("li")]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k][:-1]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/buton_tengah.csv", index=False)
                self.success_list.append("buton_tengah success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("buton_tengah failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("buton_tengah failed..\n\n")


    def buton_utara(self):
        try:
            soup = self.get_url(self.dprd_url515)
            list_nama = [x.text.split("\n") for x in soup.find_all("p")[10:-1]]
            list_nama = [j for sub in list_nama for j in sub][2:]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            list_nama = [k for k in list_nama if 'Dapil' not in k]
            list_nama = [k.split(". ")[-1] if ". " in k else k for k in list_nama]
            list_nama = [k.split("dari")[0] if "dari" in k else k for k in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/buton_utara.csv", index=False)
                self.success_list.append("buton_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("buton_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("buton_utara failed..\n\n")


    def kolaka(self):
        try:
            soup = self.get_url(self.dprd_url516)
            list_nama = [x.text.split("\n") for x in soup.find("div", {"class":"entry-content"}).find_all("p")[2:-2]]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            list_nama = [k for k in list_nama if 'Daerah' not in k]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/kolaka.csv", index=False)
                self.success_list.append("kolaka success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kolaka failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kolaka failed..\n\n")


    def kolaka_timur(self):
        try:
            soup = self.get_url(self.dprd_url517)
            list_nama = [x.split(")") for x in soup.find("div", {"id":"article-detail-content"}).text.split("\n")[5].split(":")[2:]]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [k.split("(")[0] for k in list_nama if '(' in k]
            list_nama = [k for k in list_nama if 'Dapil' not in k]
            list_nama = [" ".join(k.split(".")[1:]) if len(k.split(". ")) >= 2 else k.split(". ")[0] for k in list_nama]
            df = pd.DataFrame.from_dict({"nama" : list_nama})
            df["alamat"] = "No Data"
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/kolaka_timur.csv", index=False)
                self.success_list.append("kolaka_timur success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kolaka_timur failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kolaka_timur failed..\n\n")


    def kolaka_utara(self):
        try:
            soup = self.get_url(self.dprd_url517)
            df = pd.read_html(self.dprd_url518)[0]
            df.columns = ["no", "nama", "partai", "suara", "kursi", "dapil"]
            df = df.iloc[1:]
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/kolaka_utara.csv", index=False)
                self.success_list.append("kolaka_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kolaka_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kolaka_utara failed..\n\n")


    def konawe(self):
        self.error_desc.append("no data")
        self.failed_list.append("konawe failed..\n\n")


    def konawe_kep(self):
        try:
            soup = self.get_url(self.dprd_url520)
            list_nama = [x.text.split("Partai")[0] for x in soup.find("ol").find_all("li")]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/konawe_kep.csv", index=False)
                self.success_list.append("konawe_kep success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("konawe_kep failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("konawe_kep failed..\n\n")


    def konawe_selatan(self):
        try:
            soup = self.get_url(self.dprd_url521)
            list_nama = list(set([y.text.split("perolehan ")[0] for x in soup.find_all("ol") for y in x.find_all("li")]))
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/konawe_selatan.csv", index=False)
                self.success_list.append("konawe_selatan success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("konawe_selatan failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("konawe_selatan failed..\n\n")


    def konawe_utara(self):
        try:
            soup = self.get_url(self.dprd_url522)
            list_nama = [x.text.split("\n") for x in soup.find("div", {"class":"td-post-content"}).find_all("p")[7:]]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [x.split("\n") for x in list_nama]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [j.split("/")[0].replace("- ","") for j in list_nama if "/" in j]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/konawe_utara.csv", index=False)
                self.success_list.append("konawe_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("konawe_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("konawe_utara failed..\n\n")


    def muna(self):
        try:
            soup = self.get_url(self.dprd_url523)
            list_nama = [x.text.split("(")[0].split(". ")[1] for x in soup.find_all("p")[11:-4][0] if "(" in x]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/muna.csv", index=False)
                self.success_list.append("muna success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("muna failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("muna failed..\n\n")


    def muna_barat(self):
        try:
            soup = self.get_url(self.dprd_url524)
            list_nama = [x.text.split(":")[1].split(")") for x in soup.find_all("p")[9:12]]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [x.split("(")[0].replace(" dan ","") for x in list_nama if "(" in x]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/muna_barat.csv", index=False)
                self.success_list.append("muna_barat success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("muna_barat failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("muna_barat failed..\n\n")


    def wakatobi(self):
        try:
            soup = self.get_url(self.dprd_url525)
            list_nama = [str(x).split("br") for x in soup.find_all("p", {"style":"text-align: justify;"})[11:]]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [j for j in list_nama if "/>" in j][2:]
            list_nama = [" ".join(x.split(".")[1:]) if len(x.split("."))>=2 else x.split(".")[1] for x in list_nama]
            list_nama = [x.split("<")[0] for x in list_nama]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/wakatobi.csv", index=False)
                self.success_list.append("wakatobi success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("wakatobi failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("wakatobi failed..\n\n")


    def baubau(self):
        try:
            soup = self.get_url(self.dprd_url526)
            list_nama = [x.find_all("li") for x in soup.find_all("ol")]
            list_nama = [x.text.split("(")[0] for sub in list_nama for x in sub]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/baubau.csv", index=False)
                self.success_list.append("baubau success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("baubau failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("baubau failed..\n\n")


    def kendari(self):
        try:
            soup = self.get_url(self.dprd_url527)
            list_nama = [x.find_all("li") for x in soup.find_all("ol")]
            list_nama = [j.text for sub in list_nama for j in sub]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tenggara/kendari.csv", index=False)
                self.success_list.append("kendari success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("kendari failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("kendari failed..\n\n")


    def banggai(self):
        try:
            soup = self.get_url(self.dprd_url528)
            list_nama = [x.text for x in soup.find("div", {"class":"content-inner"}).find_all("p")][4:]
            list_nama = [x.split("berikut : ")[-1].split(",") for x in list_nama]
            list_nama = [j for sub in list_nama for j in sub]
            list_remove = [5,11,35]
            for x in list_remove:
                del list_nama[x]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tengah/banggai.csv", index=False)
                self.success_list.append("banggai success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("banggai failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("banggai failed..\n\n")


    def banggai_kep(self):
        self.error_desc.append("no data")
        self.failed_list.append("banggai_kep failed..\n\n")


    def banggai_laut(self):
        self.error_desc.append("no data")
        self.failed_list.append("banggai_laut failed..\n\n")


    def buol(self):
        self.error_desc.append("no data")
        self.failed_list.append("buol failed..\n\n")


    def dongala(self):
        self.error_desc.append("no data")
        self.failed_list.append("dongala failed..\n\n")


    def morowali(self):
        try:
            soup = self.get_url(self.dprd_url533)
            list_nama = [x.text.lower().split("dari")[0] for x in soup.find("ol").find_all("li")]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tengah/morowali.csv", index=False)
                self.success_list.append("morowali success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("morowali failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("morowali failed..\n\n")


    def morowali_utara(self):
        try:
            soup = self.get_url(self.dprd_url534)
            list_nama = [x.text.split("\n") for x in soup.find_all("p")[5:8]]
            list_nama = [j.split(" - ")[0].split(". ")[1] for sub in list_nama for j in sub[1:]]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tengah/morowali_utara.csv", index=False)
                self.success_list.append("morowali_utara success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("morowali_utara failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("morowali_utara failed..\n\n")


    def parigi_moutong(self):
        try:
            soup = self.get_url(self.dprd_url535)
            list_nama = [x.text.split(",") for x in soup.find_all("p")[6:10]]
            list_nama = [j.split("adalah ")[-1] for sub in list_nama for j in sub]
            list_nama = [j.split("(")[0].replace("\xa0","") for j in list_nama if "(" in j]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tengah/parigi_moutong.csv", index=False)
                self.success_list.append("parigi_moutong success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("parigi_moutong failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("parigi_moutong failed..\n\n")


    def poso(self):
        try:
            soup = self.get_url(self.dprd_url536)
            list_nama = [x.text.lower() for x in soup.find("div", {"class":"StoryRenderer__EditorWrapper-th08cs-0 ZDWZV"}).find_all("span")][11:]
            list_nama = list(set([x for x in list_nama if "calon terpilih" in x]))
            list_nama = [x.split("calon terpilih")[1] for x in list_nama]
            list_nama = [x.split("dan") for x in list_nama]
            list_nama = [j.replace("yaitu,","") for sub in list_nama for j in sub]
            list_nama = [x.split("yakni")[-1] for x in list_nama]
            list_nama = [j.replace(":","") for j in list_nama]
            list_nama = [j.replace(", ","") for j in list_nama]
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tengah/poso.csv", index=False)
                self.success_list.append("poso success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("poso failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("poso failed..\n\n")


    def sigi(self):
        self.error_desc.append("no data")
        self.failed_list.append("sigi failed..\n\n")


    def tojo_unauna(self):
        try:
            soup = self.get_url(self.dprd_url538)
            dapil1 = [x.text.split("\n") for x in soup.find_all("p")][12:13]
            dapil1 = [" ".join(j.split(".")[1:]) if len(j.split(".")) >=2 else j.split(".") for sub in dapil1 for j in sub][1:]
            dapil1 = [x.split("(")[0].replace("\xa0", "") for x in dapil1]
            list_nama = [y.text.split("\n") for x in soup.find_all("ol") for y in x.find_all("li")]
            list_nama = [j for sub in list_nama for j in sub]
            list_nama = [x.split("(")[0] for x in list_nama]
            list_nama = [x.split("\xa0")[-1] for x in list_nama][:-1]
            list_nama = [x.replace("4. ", "") for x in list_nama]
            list_nama.extend(dapil1)
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tengah/tojo_unauna.csv", index=False)
                self.success_list.append("tojo_unauna success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("tojo_unauna failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("tojo_unauna failed..\n\n")


    def toli_toli(self):
        try:
            soup = self.get_url(self.dprd_url539)
            list_nama = [x.text.split("\n") for x in soup.find_all("p")[7:-3]]
            list_nama = [j.split(",") for sub in list_nama for j in sub]
            list_nama = [j.split("adalah") for sub in list_nama for j in sub]
            list_nama = [j.split("dan") for sub in list_nama for j in sub]
            list_nama = [j.split("Kemudian") for sub in list_nama for j in sub]
            list_nama = [j.split("selanjutnya") for sub in list_nama for j in sub]
            list_nama = [j for sub in list_nama for j in sub][1:]
            list_nama = [x.split("(")[0] for x in list_nama]
            list_nama = [x.replace("Selanjutnya", "").lstrip() for x in list_nama]
            list_nama = list(filter(None, list_nama))
            df = pd.DataFrame.from_dict({"nama":list_nama})
            df["alamat"] = "No Data"
            df = df[["nama", "alamat"]]
            if df.shape[0] > 5:
                df.to_csv("./scrapping/result/dprd_tk2/Sulawesi Tengah/toli_toli.csv", index=False)
                self.success_list.append("toli_toli success..\n")
                return df
            else:
                self.error_desc.append("df.shape[0] < 5\n")
                self.failed_list.append("toli_toli failed..\n\n")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("toli_toli failed..\n\n")


    def palu(self):
        self.error_desc.append("no data")
        self.failed_list.append("palu failed..\n\n")


    def bolaang_mongondow_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("bolaang_mongondow_selatan failed..\n\n")


    def bolaang_mongondow_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("bolaang_mongondow_timur failed..\n\n")        
        
        
    def bolaang_mongondow_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("bolaang_mongondow_utara failed..\n\n")
        

    def sangihe(self):
        self.error_desc.append("no data")
        self.failed_list.append("sangihe failed..\n\n")        


    def Kepulauan_Siau_Tagulandang_Biaro(self):
        self.error_desc.append("no data")
        self.failed_list.append("Kepulauan_Siau_Tagulandang_Biaro failed..\n\n")


    def kepulauan_talaud(self):
        self.error_desc.append("no data")
        self.failed_list.append("kepulauan_talaud failed..\n\n")


    def minahasa(self):
        self.error_desc.append("no data")
        self.failed_list.append("minahasa failed..\n\n")


    def minahasa_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("minahasa_selatan failed..\n\n")


    def minahasa_tenggara(self):
        self.error_desc.append("no data")
        self.failed_list.append("minahasa_tenggara failed..\n\n")


    def minahasa_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("minahasa_utara failed..\n\n")


    def kota_bitung(self):
        self.error_desc.append("no data")
        self.failed_list.append("kota_bitung failed..\n\n")


    def kota_kotamobagu(self):
        self.error_desc.append("no data")
        self.failed_list.append("kota_kotamobagu failed..\n\n")


    def kota_manado(self):
        self.error_desc.append("no data")
        self.failed_list.append("kota_manado failed..\n\n")


    def kota_tomohon(self):
        self.error_desc.append("no data")
        self.failed_list.append("kota_tomohon failed..\n\n")


    def majene(self):
        self.error_desc.append("no data")
        self.failed_list.append("majene failed..\n\n")


    def mamasa(self):
        self.error_desc.append("no data")
        self.failed_list.append("mamasa failed..\n\n")


    def mamuju(self):
        self.error_desc.append("no data")
        self.failed_list.append("mamuju failed..\n\n")


    def mamuju_tengah(self):
        self.error_desc.append("no data")
        self.failed_list.append("mamuju_tengah failed..\n\n")


    def mamuju_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("mamuju_utara failed..\n\n")


    def polewali_mandar(self):
        self.error_desc.append("no data")
        self.failed_list.append("polewali_mandar failed..\n\n")


    def mamuju_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("mamuju_kota failed..\n\n")


    def buru(self):
        self.error_desc.append("no data")
        self.failed_list.append("buru failed..\n\n")


    def buru_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("buru_selatan failed..\n\n")


    def kep_aru(self):
        self.error_desc.append("no data")
        self.failed_list.append("kep_aru failed..\n\n")


    def maluku_barat_daya(self):
        self.error_desc.append("no data")
        self.failed_list.append("maluku_barat_daya failed..\n\n")


    def maluku_tengah(self):
        self.error_desc.append("no data")
        self.failed_list.append("maluku_tengah failed..\n\n")


    def maluku_tenggara(self):
        self.error_desc.append("no data")
        self.failed_list.append("maluku_tenggara failed..\n\n")


    def maluku_tenggara_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("maluku_tenggara_barat failed..\n\n")


    def seram_bagian_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("seram_baian_barat failed..\n\n")


    def seram_bagian_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("seram_bagian_timur failed..\n\n")


    def ambon(self):
        self.error_desc.append("no data")
        self.failed_list.append("ambon failed..\n\n")


    def tual(self):
        self.error_desc.append("no data")
        self.failed_list.append("tual failed..\n\n")


    def mahamera_barat(self):
        self.error_desc.append("no data")
        self.failed_list.append("mahamera_barat failed..\n\n")


    def mahamera_tengah(self):
        self.error_desc.append("no data")
        self.failed_list.append("mahamera_tengah failed..\n\n")


    def mahamera_utara(self):
        self.error_desc.append("no data")
        self.failed_list.append("mahamera_utara failed..\n\n")


    def mahamera_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("mahamera_selatan failed..\n\n")


    def mahamera_timur(self):
        self.error_desc.append("no data")
        self.failed_list.append("mahamera_timur failed..\n\n")


    def kep_sula(self):
        self.error_desc.append("no data")
        self.failed_list.append("kep_sula failed..\n\n")


    def pulau_morotai(self):
        self.error_desc.append("no data")
        self.failed_list.append("pulau_morotai failed..\n\n")


    def pulau_taliabu(self):
        self.error_desc.append("no data")
        self.failed_list.append("pulau_taliabu failed..\n\n")


    def ternate_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("ternate_kota failed..\n\n")


    def tidore_kep(self):
        self.error_desc.append("no data")
        self.failed_list.append("tidore_kep failed..\n\n")


    def asmat(self):
        self.error_desc.append("no data")
        self.failed_list.append("asmat failed..\n\n")


    def biak_numfor(self):
        self.error_desc.append("no data")
        self.failed_list.append("biak_numfor failed..\n\n")


    def boven_digoel(self):
        self.error_desc.append("no data")
        self.failed_list.append("boven_digoel failed..\n\n")


    def deiyai(self):
        self.error_desc.append("no data")
        self.failed_list.append("deiyai failed..\n\n")


    def dogiyai(self):
        self.error_desc.append("no data")
        self.failed_list.append("dogiyai failed..\n\n")


    def intai_jaya(self):
        self.error_desc.append("no data")
        self.failed_list.append("intai_jaya failed..\n\n")


    def jayapura(self):
        self.error_desc.append("no data")
        self.failed_list.append("jayapura failed..\n\n")


    def jayawijaya(self):
        self.error_desc.append("no data")
        self.failed_list.append("jayawijaya failed..\n\n")


    def keerom(self):
        self.error_desc.append("no data")
        self.failed_list.append("keerom failed..\n\n")


    def kepulauan_yapen(self):
        self.error_desc.append("no data")
        self.failed_list.append("kepulauan_yapen failed..\n\n")


    def lanny_jaya(self):
        self.error_desc.append("no data")
        self.failed_list.append("lanny_wijaya failed..\n\n")


    def mamberamo_raya(self):
        self.error_desc.append("no data")
        self.failed_list.append("mamberamo_raya failed..\n\n")


    def mamberamo_tengah(self):
        self.error_desc.append("no data")
        self.failed_list.append("mamberamo_tengah failed..\n\n")


    def mappi(self):
        self.error_desc.append("no data")
        self.failed_list.append("mappi failed..\n\n")


    def merauke(self):
        self.error_desc.append("no data")
        self.failed_list.append("merauke failed..\n\n")


    def mimika(self):
        self.error_desc.append("no data")
        self.failed_list.append("mimika failed..\n\n")


    def nabire(self):
        self.error_desc.append("no data")
        self.failed_list.append("nabire failed..\n\n")


    def nduga(self):
        self.error_desc.append("no data")
        self.failed_list.append("nduga failed..\n\n")


    def paniai(self):
        self.error_desc.append("no data")
        self.failed_list.append("paniai failed..\n\n")


    def pegunungan_bintang(self):
        self.error_desc.append("no data")
        self.failed_list.append("pegunungan_bintang failed..\n\n")


    def puncak(self):
        self.error_desc.append("no data")
        self.failed_list.append("puncak failed..\n\n")


    def puncak_jaya(self):
        self.error_desc.append("no data")
        self.failed_list.append("puncak_jaya failed..\n\n")


    def sarmi(self):
        self.error_desc.append("no data")
        self.failed_list.append("sarmi failed..\n\n")


    def supiori(self):
        self.error_desc.append("no data")
        self.failed_list.append("supiori failed..\n\n")


    def tolikara(self):
        self.error_desc.append("no data")
        self.failed_list.append("tolikara failed..\n\n")


    def waropen(self):
        self.error_desc.append("no data")
        self.failed_list.append("waropen failed..\n\n")


    def yahukimo(self):
        self.error_desc.append("no data")
        self.failed_list.append("yahukimo failed..\n\n")


    def yalimo(self):
        self.error_desc.append("no data")
        self.failed_list.append("yalimo failed..\n\n")


    def jayapura_kota(self):
        self.error_desc.append("no data")
        self.failed_list.append("jayapura_kota failed..\n\n")


    def fakfak(self):
        self.error_desc.append("no data")
        self.failed_list.append("fakfak failed..\n\n")


    def kaimana(self):
        self.error_desc.append("no data")
        self.failed_list.append("kaimana failed..\n\n")


    def manokwari(self):
        self.error_desc.append("no data")
        self.failed_list.append("manokwari failed..\n\n")


    def manokwari_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("manokwari_selatan failed..\n\n")


    def maybrat(self):
        self.error_desc.append("no data")
        self.failed_list.append("maybrat failed..\n\n")


    def pegunungan_arfak(self):
        self.error_desc.append("no data")
        self.failed_list.append("pegunungan_arfak failed..\n\n")


    def raja_ampat(self):
        self.error_desc.append("no data")
        self.failed_list.append("raja_ampat failed..\n\n")


    def raja_ampat(self):
        self.error_desc.append("no data")
        self.failed_list.append("raja_ampat failed..\n\n")


    def sorong(self):
        self.error_desc.append("no data")
        self.failed_list.append("sorong failed..\n\n")


    def sorong_selatan(self):
        self.error_desc.append("no data")
        self.failed_list.append("sorong_selatan failed..\n\n")


    def tambrauw(self):
        self.error_desc.append("no data")
        self.failed_list.append("tambrauw failed..\n\n")


    def teluk_bintuni(self):
        self.error_desc.append("no data")
        self.failed_list.append("teluk_bintuni failed..\n\n")


    def teluk_wondama(self):
        self.error_desc.append("no data")
        self.failed_list.append("teluk_wondama failed..\n\n")


    def get_all_data(self):
        self.aceh_barat()
        self.aceh_baratdaya()
        self.aceh_besar()
        self.aceh_jaya()
        self.aceh_selatan()
        self.aceh_singkil()
        self.aceh_tamiang()
        self.aceh_tengah()
        self.aceh_tenggara()
        self.aceh_timur()
        self.aceh_utara()
        self.bener_meriah()
        self.bireuen()
        self.gayo_lues()
        self.nagan_raya()
        self.pidie()
        self.pidie_jaya()
        self.simeulue()
        self.banda_aceh()
        self.langsa()
        self.lhokseumawe()
        self.sabang()
        self.subulussalam()
        self.asahan()
        self.batubara()
        self.dairi()
        self.deli_serdang()
        self.humbang_hasundutan()
        self.karo()
        self.labuhanbatu()
        self.labuhanbatu_selatan()
        self.labuhanbatu_utara()
        self.langkat()
        self.mandailing_natal()
        self.nias()
        self.nias_barat()
        self.nias_selatan()
        self.nias_utara()
        self.padang_lawas()
        self.padang_lawas_utara()
        self.pakpak_bharat()
        self.samosir()
        self.serdang_bedagai()
        self.simalungun()
        self.tapanuli_selatan()
        self.tapanuli_tengah()
        self.tapanuli_utara()
        self.toba_samosir()
        self.binjai()
        self.gunungsitoli()
        self.medan()
        self.padangsidempuan()
        self.pematangsiantar()
        self.sibolga()
        self.tanjungbalai()
        self.tebingtinggi()
        self.agam()
        self.dharmasraya()
        self.kep_mentawai()
        self.lima_puluh_kota()
        self.padang_pariaman()
        self.pasaman()
        self.pasaman_barat()
        self.pesisir_selatan()
        self.sijunjung()
        self.solok()
        self.solok_selatan()
        self.tanah_datar()
        self.bukittinggi()
        self.padang()
        self.padangpanjang()
        self.pariaman()
        self.payakumbuh()
        self.sawahlunto()
        self.solok()
        self.banyuasin()
        self.empat_lawang()
        self.lahat()
        self.muara_enim()
        self.musi_banyuasin()
        self.musi_rawas()
        self.musi_rawas_utara()
        self.ogan_ilir()
        self.ogan_komering_ilir()
        self.ogan_komering_ulu()
        self.ogan_komering_ulu_selatan()
        self.ogan_komering_ulu_timur()
        self.penukal_abab_lematang_ilir()
        self.lubuklinggau()
        self.pagaralam()
        self.palembang()
        self.prabumulih()
        self.bengkalis()
        self.indragiri_hilir()
        self.indragiri_hulu()
        self.kampar()
        self.kep_meranti()
        self.kuantan_singingi()
        self.pelalawan()
        self.rokan_hilir()
        self.rokan_hulu()
        self.siak()
        self.dumai()
        self.pekanbaru()
        self.bintan()
        self.karimun()
        self.kep_anambas()
        self.lingga()
        self.natuna()
        self.batam()
        self.tanjung_pinang()
        self.batang_hari()
        self.bungo()
        self.kerinci()
        self.merangin()
        self.muaro_jambi()
        self.sarolangun()
        self.tanjung_jabung_barat()
        self.tanjung_jabung_timur()
        self.tebo()
        self.jambi()
        self.sungai_penuh()
        self.bungkulu_selatan()
        self.bengkulu_tengah()
        self.bengkulu_utara()
        self.kaur()
        self.kepahiang()
        self.lebong()
        self.muko_muko()
        self.rejang_lebong()
        self.seluma()
        self.bengkulu()
        self.bangka()
        self.bangka_barat()
        self.bangka_selatan()
        self.bangka_tengah()
        self.belitung()
        self.belitung_timur()
        self.pangkal_pinang()
        self.lampung_tengah()
        self.lampung_utara()
        self.lampung_selatan()
        self.lampung_barat()
        self.lampung_timur()
        self.mesuji()
        self.pesawaran()
        self.pesisir_barat()
        self.pringsewu()
        self.tulang_bawang()
        self.tulang_bawang_barat()
        self.tanggamus()
        self.way_kanan()
        self.bandar_lampung()
        self.metro_kota()
        self.lebak()
        self.pandeglang()
        self.serang()
        self.tangerang_kabupaten()
        self.cilegon()
        self.serang()
        self.tangerang()
        self.tangerang_kabupaten()
        self.bandung()
        self.bandung_barat()
        self.bekasi_kabupaten()
        self.bogor_kabupaten()
        self.ciamis_kabupaten()
        self.cianjur_kabupaten()
        self.cirebon_kabupaten()
        self.garut_kabupaten()
        self.indramayu_kabupaten()
        self.karawang_kabupaten()
        self.kuningan_kabupaten()
        self.majalengka_kabupaten()
        self.pangandaran_kabupaten()
        self.purwakarta_kabupaten()
        self.subang_kabupaten()
        self.sukabumi_kabupaten()
        self.sumedang_kabupaten()
        self.tasikmalaya_kabupaten()
        self.bandung_kota()
        self.banjar_kota()
        self.bekasi_kota()
        self.bogor_kota()
        self.cimahi_kota()
        self.cirebon_kota()
        self.depok_kota()
        self.sukabumi_kota()
        self.tasikmalaya_kota()
        self.banjarnegara_kabupaten()
        self.banyumas_kabupaten()
        self.batang_kabupaten()
        self.blora_kabupaten()
        self.boyolali_kabupaten()
        self.brebes_kabupaten()
        self.cilacap_kabupaten()
        self.demak_kabupaten()
        self.grobogan_kabupaten()
        self.jepara_kabupaten()
        self.karanganyar_kabupaten()
        self.kebumen_kabupaten()
        self.kendal_kabupaten()
        self.klaten_kabupaten()
        self.kudus_kabupaten()
        self.magelang_kabupaten()
        self.pati_kabupaten()
        self.pekalongan_kabupaten()
        self.pemalang_kabupaten()
        self.purbalingga_kabupaten()
        self.purworejo_kabupaten()
        self.rembang_kabupaten()
        self.semarang_kabupaten()
        self.sragen_kabupaten()
        self.sukoharjo_kabupaten()
        self.tegal_kabupaten()
        self.temanggung_kabupaten()
        self.wonogiri_kabupaten()
        self.wonosobo_kabupaten()
        self.magelang_kabupaten()
        self.pekalongan_kabupaten()
        self.salatiga_kabupaten()
        self.salatiga_kabupaten()
        self.surakarta_kabupaten()
        self.tegal_kota()
        self.bangkalan_kabupaten()
        self.banyuwangi_kabupaten()
        self.blitar_kabupaten()
        self.bojonegoro_kabupaten()
        self.bondowoso_kabupaten()
        self.gresik_kabupaten()
        self.jember_kabupaten()
        self.jombang_kabupaten()
        self.kediri_kabupaten()
        self.lamongan_kabupaten()
        self.lumanjang_kabupaten()
        self.madiun_kabupaten()
        self.magetan_kabupaten()
        self.malang_kabupaten()
        self.mojokerto_kabupaten()
        self.nganjuk_kabupaten()
        self.ngawi_kabupaten()
        self.pacitan_kabupaten()
        self.pamekasan_kabupaten()
        self.pasuruan_kabupaten()
        self.ponorogo_kabupaten()
        self.probolinggo_kabupaten()
        self.sampang_kabupaten()
        self.sidoarjo_kabupaten()
        self.situbondo_kabupaten()
        self.sumenep_kabupaten()
        self.trenggalek_kabupaten()
        self.tuban_kabupaten()
        self.tulungagung_kabupaten()
        self.batu_kota()
        self.blitar_kota()
        self.kediri_kota()
        self.madiun_kota()
        self.malang_kota()
        self.mojokerto_kota()
        self.pasuruan_kota()
        self.probolinggo_kota()
        self.surabaya_kota()
        self.jakarta_barat()
        self.jakarta_pusat()
        self.jakarta_selatan()
        self.jakarta_timur()
        self.jakarta_utara()
        self.kep_seribu()
        self.bantul()
        self.gunungkidul()
        self.kulonprogo()
        self.sleman()
        self.yogyakarta_kota()
        self.badung()
        self.bangli()
        self.buleleng_kabupaten()
        self.gianyar_kabupaten()
        self.jembrana()
        self.karangasem_kabupaten()
        self.klungkung_kabupaten()
        self.tabanan_kabupaten()
        self.denpasar_kabupaten()
        self.bima_kabupaten()
        self.dompu_kabupaten()
        self.lombok_barat()
        self.lombok_tengah()
        self.lombok_timur()
        self.lombok_utara()
        self.sumbawa()
        self.sumbawa_barat()
        self.bima()
        self.mataram()
        self.alor()
        self.belu()
        self.ende()
        self.flores_timur()
        self.kupang()
        self.lembata()
        self.malaka()
        self.manggarai()
        self.manggarai_barat()
        self.manggarai_timur()
        self.ngada()
        self.nagekeo()
        self.rote_ndao()
        self.sabu_raijua()
        self.sikka()
        self.sumba_barat()
        self.sumba_barat_daya()
        self.sumba_tengah()
        self.sumba_timur()
        self.timor_tengah_selatan()
        self.timor_tengah_utara()
        self.kupang_kota()
        self.bengkayang()
        self.kapuas_hulu()
        self.kayong_utara()
        self.ketapang()
        self.kuburaya()
        self.landak()
        self.melawi()
        self.mempawah()
        self.sambas()
        self.sanggau()
        self.sekadau()
        self.sintang()
        self.pontianak()
        self.singkawang()
        self.balangan()
        self.banjar()
        self.barito_kuala()
        self.hulu_sungai_selatan()
        self.hulu_sungai_tengah()
        self.hulu_sungai_utara()
        self.kotabaru()
        self.tabalong()
        self.tanah_bumbu()
        self.tanah_laut()
        self.tapin()
        self.banjarbaru()
        self.banjarmasin()
        self.barito_selatan()
        self.barito_timur()
        self.barito_utara()
        self.gunung_mas()
        self.kapuas()
        self.katingan()
        self.kotawaringin_barat()
        self.kotawaringin_timur()
        self.lamandau()
        self.murungraya()
        self.pulaupisau()
        self.sukamara()
        self.seruyan()
        self.palangkaraya()
        self.murungraya()
        self.berau()
        self.kutai_barat()
        self.kutai_kartanegara()
        self.kutai_timur()
        self.mahakam_ulu()
        self.paser()
        self.penajam_paser_utara()
        self.balikpapan_kota()
        self.bontang_kota()
        self.samarinda_kota()
        self.bulungan()
        self.malinau()
        self.nunukan()
        self.tana_tidung()
        self.tarakan()
        self.boalemo()
        self.bone_bolango()
        self.gorontalo_kabupaten()
        self.gorontalo_utara()
        self.pohuwato()
        self.gorontalo_kota()
        self.bantaeng()
        self.barru_kabupaten()
        self.bone_kabupaten()
        self.bulukumba()
        self.enrekang()
        self.gowa()
        self.jeneponto()
        self.kep_selayar()
        self.luwu()
        self.luwu_timur()
        self.luwu_utara()
        self.pangkajene_dan_kepulauan()
        self.pinrang()
        self.sidenreng_rappang()
        self.sinjai()
        self.soppeng()
        self.takalar()
        self.tana_taroja()
        self.taroja_utara()
        self.wajo()
        self.makasar()
        self.palopo_kota()
        self.parepare_kota()
        self.bombana()
        self.buton()
        self.buton_selatan()
        self.buton_tengah()
        self.buton_utara()
        self.kolaka()
        self.kolaka_timur()
        self.kolaka_utara()
        self.konawe()
        self.konawe_kep()
        self.konawe_selatan()
        self.konawe_utara()
        self.muna()
        self.muna_barat()
        self.wakatobi()
        self.baubau()
        self.kendari()
        self.banggai()
        self.banggai_kep()
        self.banggai_laut()
        self.buol()
        self.dongala()
        self.morowali()
        self.morowali_utara()
        self.parigi_moutong()
        self.poso()
        self.sigi()
        self.tojo_unauna()
        self.toli_toli()
        self.palu()
        self.bolaang_mongondow_selatan()
        self.bolaang_mongondow_timur()
        self.bolaang_mongondow_utara()
        self.sangihe()
        self.Kepulauan_Siau_Tagulandang_Biaro()
        self.kepulauan_talaud()
        self.minahasa()
        self.minahasa_selatan()
        self.minahasa_tenggara()
        self.minahasa_utara()
        self.kota_bitung()
        self.kota_kotamobagu()
        self.kota_manado()
        self.kota_tomohon()
        self.majene()
        self.mamasa()
        self.mamuju()
        self.mamuju_tengah()
        self.mamuju_utara()
        self.polewali_mandar()
        self.mamuju_kota()
        self.buru()
        self.buru_selatan()
        self.kep_aru()
        self.maluku_barat_daya()
        self.maluku_tengah()
        self.maluku_tenggara()
        self.maluku_tenggara_barat()
        self.seram_bagian_barat()
        self.seram_bagian_timur()
        self.ambon()
        self.tual()
        self.mahamera_barat()
        self.mahamera_tengah()
        self.mahamera_utara()
        self.mahamera_selatan()
        self.mahamera_timur()
        self.kep_sula()
        self.pulau_morotai()
        self.pulau_taliabu()
        self.ternate_kota()
        self.tidore_kep()
        self.asmat()
        self.biak_numfor()
        self.boven_digoel()
        self.deiyai()
        self.dogiyai()
        self.intai_jaya()
        self.jayapura()
        self.jayawijaya()
        self.keerom()
        self.kepulauan_yapen()
        self.lanny_jaya()
        self.mamberamo_raya()
        self.mamberamo_tengah()
        self.mappi()
        self.merauke()
        self.mimika()
        self.nabire()
        self.nduga()
        self.paniai()
        self.pegunungan_bintang()
        self.puncak()
        self.puncak_jaya()
        self.sarmi()
        self.supiori()
        self.tolikara()
        self.waropen()
        self.yahukimo()
        self.yalimo()
        self.jayapura_kota()
        self.fakfak()
        self.kaimana()
        self.manokwari()
        self.manokwari_selatan()
        self.maybrat()
        self.pegunungan_arfak()
        self.raja_ampat()
        self.raja_ampat()
        self.sorong()
        self.sorong_selatan()
        self.tambrauw()
        self.teluk_bintuni()
        self.teluk_wondama()


        df = pd.DataFrame.from_dict({"status": self.failed_list,
                            "error desc": self.error_desc})
        df.to_excel("./log_scrapping_dprd_tk2.xlsx", index=False)









