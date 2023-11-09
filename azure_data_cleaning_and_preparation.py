import numpy as np
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup as soup
from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter
from srbai.Alati.Transliterator import transliterate_cir2lat, transliterate_lat2cir

from azure_manual_corr_skola_fct import manual_corr_lat_long, manual_corr_ulica, manual_corr_godina
from google_api_key import api_key

geolocator = GoogleV3(api_key=api_key)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0.1)

### Getting and cleaning "basic information set on schools" dataset ###

def get_and_pd_basic_data_school():
  url = "https://data.gov.rs/sr/datasets/srednje-obrazovanje-kontakt-podatsi-osnovni-podatsi/"
  page = requests.get(url)
  data = soup(page.text, "html.parser")

  link = data.find("dl", {"class": "description-list"}).parent.findNext("dd").contents[0].get("href")
  r = requests.get(link)
  osn_kontakt = r.json()

  osn_kontakt_key = next(iter(osn_kontakt.keys()))
  srednje_osn_lok = pd.json_normalize(osn_kontakt[osn_kontakt_key])

  return srednje_osn_lok

if __name__ == '__main__':
  get_and_pd_basic_data_school()

def clean_basic_data_school(srednje_osn_lok=get_and_pd_basic_data_school()):
  srednje_osn_lok["godinaOsnivanja"] = pd.Series(np.nan_to_num(pd.to_datetime(srednje_osn_lok["datumPocetkaRadaUstanove"])\
                                        .dt.year).astype(int))
  srednje_osn_lok = srednje_osn_lok.drop(labels = "datumPocetkaRadaUstanove", axis = 1)

  flt_osn_mesto = srednje_osn_lok["mesto"].isna()
  srednje_osn_lok.loc[flt_osn_mesto, "mesto"] = srednje_osn_lok.loc[flt_osn_mesto, "opstina"]

  flt_osn_maticni = srednje_osn_lok["maticniBroj"] == "08972079"
  srednje_osn_lok.loc[flt_osn_maticni, "vrstaOsnivacaPoUstanovi"] = "Приватна установа"

  flt_miss = srednje_osn_lok["PIB"] == "07148208"
  if len(srednje_osn_lok.loc[flt_miss]) == 0:
    nova_škola = {"vrstaOsnivacaPoUstanovi": "Јавна установа", "okrug": "ЗАЈЕЧАРСКИ УПРАВНИ ОКРУГ", "opstina": "ЗАЈЕЧАР", "maticniBroj": "07148208", "PIB": "101328871", "mesto": "ЗАЈЕЧАР", "nazivUstanove": 'Школа за основно и средње образовање "Јелена Мајсторовић"', "godinaOsnivanja": "1968"}
    srednje_osn_lok.loc[len(srednje_osn_lok)] = nova_škola
  else:
    pass

  return srednje_osn_lok

if __name__ == '__main__':
  clean_basic_data_school()

print("""Basic schoool data dataset is ready.
Pripremljeni su osnovni podaci o školama\n""")

### Getting and cleaning "detailed information set on schools" dataset ###

def get_and_pd_full_data_school():
  url = "https://data.gov.rs/sr/datasets/srednje-obrazovanje-podatsi-o-lokatsijama-i-objektima-prostorijama-1/"
  page = requests.get(url)
  data = soup(page.text, "html.parser")

  link = data.find("dl", {"class": "description-list"}).parent.findNext("dd").contents[0].get("href")
  r = requests.get(link)
  sve_kontakt = r.json()

  sve_kontakt_key = next(iter(sve_kontakt.keys()))
  srednje_sve_lok = pd.json_normalize(sve_kontakt[sve_kontakt_key])

  return srednje_sve_lok

if __name__ == '__main__':
  get_and_pd_full_data_school()


def clean_full_data_school(srednje_sve_lok=get_and_pd_full_data_school(), srednje_osn_lok=clean_basic_data_school()):
  # Fixing column "okrug" (county)
  flt_okrug = srednje_sve_lok["okrug"].isna()

  try:
    opstina = list(srednje_sve_lok.loc[flt_okrug, "opstina"])[0]

    flt_opstina = srednje_sve_lok["opstina"] == opstina
    okrug = srednje_sve_lok.loc[flt_opstina, "okrug"].unique()
    okrug = [item for item in okrug if not(pd.isnull(item)) == True][0]
    srednje_sve_lok.loc[flt_okrug, "okrug"] = okrug

  except:
    pass

  # Fixing column "nazivObjekta" (object name)
  flt_objekat = srednje_sve_lok["nazivObjekta"].isna()
  srednje_sve_lok.loc[flt_objekat, "nazivObjekta"] = srednje_sve_lok.loc[flt_objekat, "nazivUstanove"]

  # Fixing column "mesto" (city)
  popraviti_mesto = pd.merge(left=srednje_sve_lok, right = srednje_osn_lok, how = "left",on = ["nazivUstanove", "opstina"])
  srednje_sve_lok = popraviti_mesto[["vrstaOsnivacaPoUstanovi_x", "nazivUstanove", "okrug_x", "opstina", "mesto_y", "ulica", "povrsinaObjekta", "brojUcionica", "povrsinaUcionica",\
                                     "brojKuhinja", "povrsinaKuhinja", "brojBiblioteka", "brojRadionica", "povrsinaRadionica", "brojRestorana", "brojKabineta",\
                                     "povrsinaKabineta", "brojLaboratorija", "povrsinaLaboratorija", "brojFiskulturnihSala", "povrsinaFiskulturnihSala"]]
  srednje_sve_lok = srednje_sve_lok.rename(columns = {"vrstaOsnivacaPoUstanovi_x": "vrstaOsnivacaPoUstanovi", "okrug_x": "okrug", "mesto_y": "mesto"})

  # Aggregating data
  basic_col_list = ["vrstaOsnivacaPoUstanovi", "nazivUstanove", "okrug", "opstina", "mesto", "ulica"]
  srednje_sve_lok = srednje_sve_lok.groupby(basic_col_list).sum(numeric_only=True).reset_index()

  return srednje_sve_lok

if __name__ == '__main__':
  clean_full_data_school()

print("""Additonal school data dataset is ready.
Pripremljeni su dodatni podaci o školama\n""")

##### Getting and cleaning "classes and educational programs in schools" dataset #########

def get_and_pd_classes_edu_prog():
  url = "https://data.gov.rs/sr/datasets/srednje-obrazovanje-podatsi-o-odeljenjima-i-razredima/"
  page = requests.get(url)
  data = soup(page.text, "html.parser")

  link = data.find("dl", {"class": "description-list"}).parent.findNext("dd").contents[0].get("href")
  r = requests.get(link)
  classes_edu_prog = r.json()

  classes_edu_prog_key = next(iter(classes_edu_prog.keys()))
  srednje_razredi = pd.json_normalize(classes_edu_prog[classes_edu_prog_key])

  return srednje_razredi

if __name__ == '__main__':
  get_and_pd_full_data_school()

def clean_classes_edu_prog(srednje_razredi=get_and_pd_classes_edu_prog()):

  ### Standardizing program names for educational programs ###
  standardizacija_dict = {"-ученици са ЛМО": "(ученици са сметњама у развоју-лако ментално ометени у развоју)"
                        , "(за ученике са поремећајем у понашању)": " (ученици са сметњама у друштвеном понашању)"
                        , "( за ученике са сметњама у друштвеном понашању)": " (ученици са сметњама у друштвеном понашању)"
                        , "-ученици оштећеног вида": " (ученици оштећеног вида)"
                        , " – поремећај у понашању": " (ученици са сметњама у друштвеном понашању)"
                        , "-поремећај у понашању": " (ученици са сметњама у друштвеном понашању)"
                        , " (за ученике са поремећајем у понашању)": " (ученици са сметњама у друштвеном понашању)"
                        , "Електрозаваривач (поремећај у понашању)": "Електрозаваривач (ученици са сметњама у друштвеном понашању)"
                        , "Шивач текстила-(поремећај у понашању)": "Шивач текстила (ученици са сметњама у друштвеном понашању)"
                        , "( (ученици са сметњама у друштвеном понашању))": "(ученици са сметњама у друштвеном понашању)"
                        , "слухом оштећени ученици": "(ученици са сметњама у развоју-наглуви и глуви)"
                        , "дорадеученици са сметњама у развоју-лако ментално ометени у развоју": "дораде (ученици са сметњама у развоју-лако ментално ометени у развоју"
                        , "(лако ментално ометени у развоју)": " (ученици са сметњама у развоју-лако ментално ометени у развоју)"
                        , " - ": "-"
                        , "Гимназија за обдарене ученике у Математичкој гимназији": "Програм за обдарене ученике у Математичкој гимназији"
                        , "Обдарени у филолошкој гимназији-живи језици": "Програм за обдарене ученике у Филолошкој гимназији-живи језици"
                        , "Обдарени ученици у филолошкој гимназији-Класични језици": "Програм за обдарене ученике у Филолошкој гимназији-класични језици"
                        , "Правилник о наставном плану и програму а за гимназију за ученике са посебним способностима за физику": "Програм за ученике са посебним способностима за физику"
                        , "Правилник о наставном плану и програму за обдарене ученике у Рачунарској гимназији у Београду": "Програм за обдарене ученике у Рачунарској гимназији у Београду"
                        , "Обдарени ученици у рачунарској гимназији": "Програм за ученике са посебним способностима за рачунарство и информатику"
                        , "Оператер машинске обраде резањем": "Оператер машинске обраде"
                        , "Текстилни радник": "Текстилни техничар"
                        , "Стоматолошка сестра техничар": "Стоматолошка сестра-техничар"
                        , "Шумар": "Шумарски техничар"
                        , "Електромеханичар за термичке и расхладне уређаје": "Електротехничар за термичке и расхладне уређаје"
                        , "Авио-техничар за електро опрему ваздухоплова": "Авио-техничар за електронску опрему ваздухоплова"
                        , "Механичар моторних возила": "Машински техничар моторних возила"
                        , "Финансијски техничар": "Финансијско-рачуноводствени техничар"
                        , "Средња музичка школа-одсек ВОКАЛНО-ИНСТРУМЕНТАЛНИ-МУЗИЧКИ ИЗВОЂАЧ": "Музички извођач"
                        , "Техничар за полимере по дуалном моделу": "Техничар за полимере"
                        , "Правни техничар": "Правно-пословни техничар"
                        , " (поремећај у понашању)": " (ученици са сметњама у друштвеном понашању)"
                        , "-(поремећај у понашању)": " (ученици са сметњама у друштвеном понашању)"
                        , "( (ученици са сметњама у друштвеном понашању))": "(ученици са сметњама у друштвеном понашању)"
                        , "– поремећај у понашању": "(ученици са сметњама у друштвеном понашању)"
                        , "(за ученике са поремећајем у понашању)": "(ученици са сметњама у друштвеном понашању)"
                        , "( за ученике са сметњама у друштвеном понашању)": "(ученици са сметњама у друштвеном понашању)"
                        , "Играч-савремена игра": "Играч савремног плеса"
                        , "Играч савремене игре": "Играч савремног плеса"
                        }

  pravilnik = {"Правилник о наставном плану и програму за обдарене ученике у филолошкој гимназији живи језици": "Програм за обдарене ученике у Филолошкој гимназији-живи језици"
            , "Правилник о наставном плану и програму за обдарене ученике у математичкој гимназији": "Програм за обдарене ученике у Математичкој гимназији"
            , "Правилник о наставном плану и програму за гимназију за ученике са посебним способностима за рачунарство и информатику": "Програм за ученике са посебним способностима за рачунарство и информатику"
            , "Аутолимар – поремећај у понашању": "Аутолимар (ученици са сметњама у друштвеном понашању)"
            , "Педикир-маникир (за ученике са поремећајем у понашању)": "Педикир-маникир (ученици са сметњама у друштвеном понашању)"
            , "Књиговезац слухом оштећени ученици": "Књиговезац (ученици са сметњама у развоју-наглуви и глуви)"
            , "Цвећар – вртлар": "Цвећар-вртлар"
            , "Руковалац грађевинском механизацијом школски модел": "Руковалац грађевинском механизацијом"
            , "Електротехничар информационих технологија-": "Електротехничар информационих технологија"
            , "Физиотерапеутски техничар-ученици оштећеног вида": "Физиотерапеутски техничар (ученици са сметњама у развоју-слепи и слабовиди)"
            , "Техничар за полимере по дуалном моделу": "Техничар за полимере"
            , "Одсек КЛАСИЧАН БАЛЕТ": "Играч класичног балета"
            , "Службеник осигурања": "Службеник у банкарству и осигурању"
            , "Помоћник аутолакирера(лако ментално ометени у развоју)": "Помоћник аутолакирера (ученици са сметњама у развоју-лако ментално ометени у развоју)"
            , "Друштвено-језички смер": "Гимназија, друштвено-језички смер"
            , "Природно-математички смер": "Гимназија, природно-математички смер"
            , "Гимназија општи тип": "Гимназија, општи тип"
            , "Гимназија друштвено-језички смер": "Гимназија, друштвено-језички смер"
            , "Гимназија природно-математички смер": "Гимназија, природно-математички смер"
            , "Средња музичка школа-одсек ВОКАЛНО-ИНСТРУМЕНТАЛНИ-МУЗИЧКИ ИЗВОЂАЧЏЕЗ МУЗИКА":"Музички звођач џез музике"
            , "Средња музичка школа-ОДСЕК ЗА МУЗИЧКУ ПРОДУКЦИЈУ И СНИМАЊЕ ЗВУКА-ДИЗАЈНЕР ЗВУКА":"Дизајнер звука"
            , "Средња музичка школа-ОДСЕК ЗА ЦРКВЕНЗ МУЗИКУ-МУЗИЧКИ ИЗВОЂАЧ ЦРКВЕНЕ МУЗИКЕ":"Музички Извођач црквене музике"
            , "Средња музичка школа-ОДСЕК ТЕОРЕТСКИ-МУЗИЧКИ САРАДНИК":"Музички сарадник"
            , "Женски фризер": "Фризер - женски"
            , "Женски фризер (ученици са сметњама у друштвеном понашању)": "Фризер - женски (ученици са сметњама у друштвеном понашању)"
            , "Женски фризер-поремећај у понашању": "Фризер - женски (ученици са сметњама у друштвеном понашању)"
            , "Женски фризер (ученици са сметњама у развоју-наглуви и глуви)": "Фризер - женски (ученици са сметњама у развоју-наглуви и глуви)"
            , "Женски фризер-ученици са ЛМО": "Фризер - женски (ученици са сметњама у развоју-лако ментално ометени у развоју)"
            , "Мушки фризер": "Фризер - мушки"
            , "Мушки фризер (ученици са сметњама у развоју-лако ометени)": "Фризер - мушки (ученици са сметњама у развоју-лако ментално ометени у развоју)"
            , "Фризер": "Фризер"
            , "Фризер (ученици са сметњама у развоју-лако ментално ометени у развоју)": "Фризер (ученици са сметњама у развоју-лако ментално ометени у развоју)"
            , "Помоћник књиговезачке дорадеученици са сметњама у развоју-лако ментално ометени у развоју)": "Помоћник књиговезачке дораде (ученици са сметњама у развоју-лако ментално ометени у развоју)"
            , "Стоматолошка сестра техничар": "Стоматолошка сестра-техничар"
            , "Туристичко-хотелијерски техничар": "Угоститељко-туристички техничар"
            , "ИГРАЧ КЛАСИЧНОГ БАЛЕТА": "Играч класичног балета"
            , "ИГРАЧ НАРОДНЕ ИГРЕ": "Играч народне игре"
            , "ИГРАЧ САВРЕМЕНЕ ИГРЕ": "Играч савременог плеса"
            }

  srednje_razredi["program_mod"]= srednje_razredi["program"].str.replace("\d\d\d\d/\d\d\d\d", "", regex=True)\
                                                            .str.replace("\d\d\d\d/\d\d", "", regex=True)\
                                                            .str.replace("Дуални модел", "")\
                                                            .str.replace("(Нови ППНУ од )", "", regex=True)\
                                                            .str.replace("\.", "", regex=True)\
                                                            .str.replace("\d\d\d\d\,\d\d", "", regex=True)\
                                                            .str.replace("\(\d\d/\d\d\d\d\)", "", regex=True)\
                                                            .str.replace("\(\d\d\d\d\)", "", regex=True)\
                                                            .str.replace("\d\d/\d\d\d\d", "", regex=True)\
                                                            .str.replace("\d\d\d\d", "", regex=True)\
                                                            .str.replace("иновиран ПП", "", regex=True)\
                                                            .str.replace("иновиран", "", regex=True)\
                                                            .str.replace("Дуални", "", regex=True)\
                                                            .str.replace("\,$", "", regex=True)\
                                                            .str.replace("(Нови ППНУ )", "", regex=True)\
                                                            .str.replace("(дуални модел)", "", regex=True)\
                                                            .str.replace("оглед", "", regex=True)\
                                                            .str.replace("()", "", regex=True)\
                                                            .str.replace("\(\)", "", regex=True)\
                                                            .str.replace("[[-][A-Z][-]]", "", regex=True)\
                                                            .str.replace("[ ,]$", "", regex=True)\
                                                            .str.replace(" - ", "-", regex=True)\
                                                            .str.replace(" - ", "-", regex=False)\
                                                            .str.replace(" -", "", regex=True)\
                                                            .str.replace(" ,", "", regex=True)\
                                                            .str.replace("\Z-", "", regex=True)\
                                                            .str.replace("Средња балетска школа-", "")\
                                                            .str.replace("-преведени", "")\
                                                            .str.replace("Винар", "винар")\
                                                            .str.replace("- нови наставни план", "")\
                                                            .str.strip("-")\
                                                            .replace(standardizacija_dict, regex=False)\
                                                            .replace(["Електромеханичар за термичке и расхладне уређаје","Механичар грејне и расхладне технике","Сервисер термичких и расхладних уређаја","Техничар грејања и климатизације","Инсталатер водовода, грејања и клима уређаја"], "Електротехничар за термичке и расхладне уређаје", regex=True)\
                                                            .replace(["Дизајнер одеће","Моделар одеће","Техничар моделар одеће","Техничар-моделар одеће","Техничар дизајна текстила","Техничар дизајна текстилних материјала"], "Техничар дизајна одеће", regex=True)\
                                                            .replace(["Музички извођач-рана музика","Музички извођач-традиционална музика","Средња музичка школа-ОДСЕК ЗА СРПСКО ТРАДИЦИОНАЛНО ПЕВАЊЕ И СВИРАЊЕ-Музички извођач српског традиционалног певања и свирања","Музички извођач ране музике"],"Музички извођач српског традиционалног певања и свирања", regex=True)\
                                                            .replace(["Средња музичка школа-ОДСЕК ЗА ЦРКВЕНЗ МУЗИКУ-МУЗИЧКИ ИЗВОЂАЧ ЦРКВЕНЕ МУЗИКЕ","Средња музичка школа-ОДСЕК ЗА ЦРКВЕНУ МУЗИКУ-ПРАВОСЛАВНИ СМЕР-Музички извођач црквене музике,православни смер"],"Музички извођач црквене музике", regex=True)\
                                                            .replace(["Средња музичка школа-ЕТНОМУЗИКОЛОШКИ ОДСЕК-МУЗИЧКИ САРАДНИК","Средња музичка школа-ОДСЕК ЗА МУЗИЧКУ ТЕОРИЈУ-Музички сарадник","Средња музичка школа-ОДСЕК ТЕОРЕТСКИ-МУЗИЧКИ САРАДНИК","Музички сарадник-теоретичар"],"Музички сарадник", regex=True)\
                                                            .replace(["Средња музичка школа-ОДСЕК ЗА МУЗИЧКУ ПРОДУКЦИЈУ И СНИМАЊЕ ЗВУКА-ДИЗАЈНЕР ЗВУКА","Средња музичка школа-ОДСЕК ЗА МУЗИЧКУ ПРОДУКЦИЈУ И ОБРАДУ ТОНА-Дизајнер звука"],"Дизајнер звука")\
                                                            .replace(["Средња музичка школа-ОДСЕК КЛАСИЧНЕ МУЗИКЕ-Извођач класичне музике","Средња музичка школа-ОДСЕК КЛАСИЧНЕ МУЗИКЕ-Музички извођач класичне музике","Музички извођач-класична музика"],"Музички извођач класичне музике", regex=True)\
                                                            .replace(["Средња музичка школа-одсек ВОКАЛНО-ИНСТРУМЕНТАЛНИ-МУЗИЧКИ ИЗВОЂАЧЏЕЗ МУЗИКА","Средња музичка школа-ОДСЕК ЗА ЏЕЗ МУЗИКУ-Музички извођач џез музике"],"Музички извођач џез музике", regex=True)\
                                                            .replace(["Tехничар телекомуникационих технологија","Техничар поштанског саобраћаја и телекомуникационих услуга","Техничар поштанског саобраћаја и телекомуникационих услуга"],"Техничар ПТТ саобраћаја", regex=True)\
                                                            .replace(["Техничар графичке дораде","Техничар дизајна графике","Техничар за графичку припрему","Техничар за обликовање графичких производа","Техничар припреме графичке производње"],"Техничар графичког дизајна")\
                                                            .replace(["Техничар за компјутерско управљање","Техничар за компјутерско управљање машина"], "Техничар за компјутерско управљање (CNC) машина")\
                                                            .replace(["Туристички техничар","Туристичко-хотелијерски техничар","Хотелијерско-ресторатерски техничар","Угоститељски техничар"], "Угоститељко-туристички техничар")\
                                                            .replace("Гимназија за ученике са посебним способностима", "Програм за обдарене ученике", regex=True)\
                                                            .replace("Програм за ученике са посебним способностима", "Програм за обдарене ученике", regex=True)\
                                                            .replace("Правилник о наставном плану и програму", "Програм", regex=True)\
                                                            .replace("Правилник о наставном плану и програму за обдарене ученике у Филолошкој гимназији живи језици", "Програм за обдарене ученике у Филолошкој гимназији-живи језици", regex=True)\
                                                            .str.strip()\
                                                            .str.replace("\s+", " ", regex=True)

  for k, v in pravilnik.items():
    flt_pravilnik = srednje_razredi["program_mod"] == k
    srednje_razredi.loc[flt_pravilnik, "program_mod"] = v

  srednje_razredi["nastavni_program"]= srednje_razredi["program_mod"].str.capitalize()

  ### Cleaning instutition type ###
  flt_osnovna = srednje_razredi["vrstaUstanove"] == "Основна школа за ученике са сметњама у развоју и инвалидитетом"
  srednje_razredi.loc[flt_osnovna, "vrstaUstanove"] = "Школа за ученике са сметњама у развоју"

  ### Cleaning classes names ###
  srednje_razredi["razred"] = srednje_razredi["razred"].replace(["I разред - СШ – више нивоа образовања", "Припремни разред I", "I разред", "/"], "1", regex=False)\
                                                       .replace(["II разред - СШ – више нивоа образовања", "II разред"], "2", regex=False)\
                                                       .replace(["III разред - СШ – више нивоа образовања", "III разред"], "3", regex=False)\
                                                       .replace(["IV разред - СШ – више нивоа образовања", "IV разред"], "4", regex=False)
  srednje_razredi = srednje_razredi.rename(columns={"jezikNastave": "jezik_nastave", "nazivUstanove": "naziv_ustanove",\
                                                    "skolskaGodina": "skolska_godina", "ukupanBrojUcenika": "broj_ucenika",\
                                                    "vrstaUstanove": "vrsta_ustanove"})
  
  return srednje_razredi

if __name__ == '__main__':
  clean_classes_edu_prog()


print("""Class dataset is ready.
Pripremljeni su podaci o razredima.""")

### Creating fact and dimentional tables ###

def founderCategory(srednje_osn_lok = clean_basic_data_school()):#-------------------OVDE DA SE MENJA
  osnivac_id = srednje_osn_lok["vrstaOsnivacaPoUstanovi"].unique()
  osnivac = pd.DataFrame(osnivac_id, columns = ["osnivacKategorija"]).reset_index()\
                                                                     .rename(mapper={"index":"osnivac_kategorija_id", "osnivacKategorija": "osnivac_kategorija"}, axis=1)

  return osnivac

if __name__ == "__main__":
  founderCategory()

def county(srednje_osn_lok = clean_basic_data_school()):
  okrug_id = srednje_osn_lok["okrug"].unique()
  okrug = pd.DataFrame(okrug_id, columns = ["okrug"]).reset_index().rename(mapper={"index": "okrug_id"}, axis=1)

  return okrug

if __name__ == "__main__":
  county()

def city(srednje_osn_lok = clean_basic_data_school()):
  opstina_id = srednje_osn_lok["opstina"].unique()
  opstina = pd.DataFrame(opstina_id, columns = ["opstina"]).reset_index().rename(mapper={"index": "opstina_id"}, axis=1)

  return opstina

if __name__ == "__main__":
  city()

def town(srednje_osn_lok = clean_basic_data_school()):
  mesto_id = srednje_osn_lok["mesto"].unique()
  mesto = pd.DataFrame(mesto_id, columns = ["mesto"]).reset_index().rename(mapper={"index": "mesto_id"}, axis=1)

  return mesto

if __name__ == "__main__":
  town()

def lecture_lang(srednje_razredi = clean_classes_edu_prog()):
  jezikNastave_id = srednje_razredi["jezik_nastave"].unique()
  jezikNastave = pd.DataFrame(jezikNastave_id, columns = ["jezik_nastave"]).sort_values(by="jezik_nastave", na_position="last")\
                                                                          .reset_index(drop=True)\
                                                                          .reset_index()\
                                                                          .rename(mapper={"index": "jezik_nastave_id"}, axis=1)

  return jezikNastave

if __name__ == "__main__":
  lecture_lang()
   
def school_type(srednje_razredi = clean_classes_edu_prog()):
  vrstaUstanove_id = srednje_razredi["vrsta_ustanove"].unique()
  vrstaUstanove = pd.DataFrame(vrstaUstanove_id, columns = ["vrsta_ustanove"]).reset_index()\
                                                                             .rename(mapper={"index": "vrsta_ustanove_id"}, axis=1)

  # Adding a school type = NaN to cover later joins with table that has a more complete dataset of schools but not this attribute
  nova_vrstaUstanove = {"vrsta_ustanove_id": len(vrstaUstanove_id), "vrsta_ustanove": np.nan}
  vrstaUstanove.loc[len(vrstaUstanove_id)] = nova_vrstaUstanove

  return vrstaUstanove

if __name__ == "__main__":
  school_type()


def school_name(srednje_osn_lok = clean_basic_data_school(), opstina=city()):
  nazivUstanove_id = srednje_osn_lok.merge(opstina, how="left", on="opstina")
  nazivUstanove_id = nazivUstanove_id[["opstina_id", "nazivUstanove"]].drop_duplicates()
  nazivUstanove = nazivUstanove_id.reset_index(drop=True).reset_index()\
                                                         .rename(mapper={"index": "naziv_ustanove_id", "nazivUstanove": "naziv_ustanove"}, axis=1)

  return nazivUstanove

if __name__ == "__main__":
  school_name()

def edu_program(srednje_razredi = clean_classes_edu_prog()):
  program_id = srednje_razredi["nastavni_program"].unique()
  program = pd.DataFrame(program_id, columns = ["nastavni_program"]).reset_index()\
                                                                    .rename(mapper={"index": "program_id"}, axis=1)
  return program

if __name__ == "__main__":
  edu_program()

def class_facttable(srednje_razredi = clean_classes_edu_prog(), program=edu_program(), nazivUstanove=school_name(), jezikNastave=lecture_lang(), opstina=city()):
  razredi_fct = srednje_razredi.merge(program, how="left", on="nastavni_program")\
                               .merge(opstina, how="left", on="opstina")\
                               .merge(nazivUstanove, how="left", on=["opstina_id", "naziv_ustanove"])\
                               .merge(jezikNastave, how="left", on="jezik_nastave")
  razredi_fct["naziv_ustanove_id"] = razredi_fct["naziv_ustanove_id"].astype(int)
  razredi_fct = razredi_fct[["naziv_ustanove_id", "program_id", "jezik_nastave_id", "skolska_godina", "razred", "odeljenje", "broj_ucenika"]]
  razredi_fct = razredi_fct.reset_index(drop=True).reset_index()\
                                                  .rename(mapper={"index": "row_id"}, axis=1)
  
  return razredi_fct

if __name__ == "__main__":
  class_facttable()

def ulica_reverse(ulica, latitude, longitude):
  if pd.notnull(ulica):
    return ulica
  if pd.isnull(latitude):
    pass
  else:
    lok = str(latitude) + ", " + str(longitude)
    nova_ulica = geolocator.reverse(lok)
    nova_ulica = transliterate_lat2cir(nova_ulica.address.split(", ")[0])
    return nova_ulica

def school_facttable(srednje_razredi=clean_classes_edu_prog(), srednje_osn_lok=clean_basic_data_school()
                    ,srednje_sve_lok=clean_full_data_school(), osnivac=founderCategory()
                    ,okrug=county(), opstina=city()
                    ,mesto=town(), vrstaUstanove=school_type()
                    ,nazivUstanove=school_name()):
  vrsta = srednje_razredi[["opstina", "naziv_ustanove", "vrsta_ustanove"]].drop_duplicates()
  skola_fct = srednje_osn_lok.merge(srednje_sve_lok, how="left", on=["opstina", "nazivUstanove"])\
                             .merge(osnivac, how="left", left_on="vrstaOsnivacaPoUstanovi_x", right_on="osnivac_kategorija")\
                             .merge(okrug, how="left", left_on="okrug_x", right_on="okrug")\
                             .merge(opstina, how="left", on="opstina")\
                             .merge(mesto, how="left", left_on="mesto_x", right_on="mesto")\
                             .merge(vrsta, how="left", left_on = ["opstina", "nazivUstanove"], right_on=["opstina", "naziv_ustanove"])\
                             .merge(vrstaUstanove, how="left", on="vrsta_ustanove")\
                             .merge(nazivUstanove, how="left", left_on=["opstina_id", "nazivUstanove"], right_on=["opstina_id", "naziv_ustanove"])


  skola_fct["infoORazredima_id"] = [0 if x==8 else 1 for x in skola_fct["vrsta_ustanove_id"]]

  skola_fct["dodatneInfoOSkoli_id"] = np.where(np.logical_xor(skola_fct["ulica"].isna(), skola_fct["povrsinaObjekta"] == 0), 0, 1)

  skola_fct = skola_fct.reset_index(drop=True).reset_index().rename(mapper={"index": "row_id"}, axis=1)

  skola_fct["skola_adr"] = skola_fct["nazivUstanove"] + ", " + skola_fct["opstina"]
  skola_fct["lokacija"] = skola_fct["skola_adr"].apply(geocode)
  skola_fct["latitude"] = skola_fct["lokacija"].apply(lambda loc: loc.latitude if loc else None)
  skola_fct["longitude"] = skola_fct["lokacija"].apply(lambda loc: loc.longitude if loc else None)

  skola_fct["nova_ulica"] = skola_fct.apply(lambda row: ulica_reverse(row["ulica"], row["latitude"], row["longitude"]), axis=1)

  to_int = np.nan_to_num(skola_fct[["naziv_ustanove_id","povrsinaObjekta","brojUcionica","povrsinaUcionica","brojKuhinja","povrsinaKuhinja"
                                   ,"brojBiblioteka","brojRadionica","povrsinaRadionica","brojRestorana","brojKabineta","povrsinaKabineta"
                                   , "brojLaboratorija","povrsinaLaboratorija","brojFiskulturnihSala","povrsinaFiskulturnihSala"]]).astype(int)
  
  skola_fct[["naziv_ustanove_id","povrsinaObjekta","brojUcionica","povrsinaUcionica","brojKuhinja","povrsinaKuhinja"
            ,"brojBiblioteka","brojRadionica","povrsinaRadionica","brojRestorana","brojKabineta","povrsinaKabineta"
            , "brojLaboratorija","povrsinaLaboratorija","brojFiskulturnihSala","povrsinaFiskulturnihSala"]] = to_int
  
  skola_fct = skola_fct[["row_id","naziv_ustanove_id","osnivac_kategorija_id","vrsta_ustanove_id","okrug_id","opstina_id","mesto_id", "nova_ulica", "latitude", "longitude", "godinaOsnivanja","maticniBroj","PIB","dodatneInfoOSkoli_id","infoORazredima_id"
                       , "povrsinaObjekta","brojUcionica","povrsinaUcionica","brojKuhinja","povrsinaKuhinja","brojBiblioteka","brojRadionica","povrsinaRadionica","brojRestorana","brojKabineta","povrsinaKabineta", "brojLaboratorija"
                       , "povrsinaLaboratorija","brojFiskulturnihSala","povrsinaFiskulturnihSala"]]
  
  skola_fct = skola_fct.rename(columns = {"nova_ulica": "ulica", "godinaOsnivanja": "godina_osnivanja", "maticniBroj": "maticni_broj", "dodatneInfoOSkoli_id":"dodatno_o_skoli_id",\
                                          "infoORazredima_id": "podaci_o_razredima_id", "povrsinaObjekta":"povrsina_objekta","brojUcionica":"broj_ucionica","povrsinaUcionica":"povrsina_ucionica",\
                                          "brojKuhinja":"broj_kuhinja","povrsinaKuhinja":"povrsina_kuhinja","brojBiblioteka":"broj_biblioteka","brojRadionica":"broj_radionica",\
                                          "povrsinaRadionica":"povrsina_radionica","brojRestorana":"broj_restorana","brojKabineta":"broj_kabineta","povrsinaKabineta":"povrsina_kabineta",\
                                          "brojLaboratorija":"broj_laboratorija","povrsinaLaboratorija":"povrsina_laboratorija","brojFiskulturnihSala":"broj_sala","povrsinaFiskulturnihSala":"povrsina_sala"
                                         })

  return skola_fct

if __name__ == "__main__":
  school_facttable()

def yn_class_info():
  dict_razredi = {"podaci_o_razredima_id": [0,1], "podaci_o_razredima": ["Не","Да"]}
  info_o_razredima = pd.DataFrame(dict_razredi)

  return info_o_razredima

if __name__ == "__main__":
  yn_class_info()

def yn_school_info():
  dict_skola = {"dodatno_o_skoli_id": [0,1], "dodatno_o_skoli": ["Не","Да"]}
  dodatno_o_skoli = pd.DataFrame(dict_skola)

  return dodatno_o_skoli

if __name__ == "__main__":
  yn_school_info()

def manual_corr_skola_fct(skola_fct = school_facttable(), manual_corr_lat_long=manual_corr_lat_long, manual_corr_godina = manual_corr_godina):
  """This is done because all addresses in Serbia do not exist in Google Maps in both cyrilic and latin alphabets
     and reverse_ulica() function fails in those cases."""
  for k, v in manual_corr_lat_long.items():
    skola_fct.loc[int(k), "latitude"] = v[0]
    skola_fct.loc[int(k), "longitude"] = v[1]

  for k, v in manual_corr_ulica.items():
    skola_fct.loc[int(k), "ulica"] = v

  for k, v in manual_corr_godina.items():
    skola_fct.loc[int(k), "godina_osnivanja"] = v
  
  
  return skola_fct

if __name__ == "__main__":
  manual_corr_skola_fct()

print("""\nFact and dimentional tables are ready.
Pripremljene su fact i dimenzione tabele.\n""")