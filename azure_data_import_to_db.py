import pandas as pd
from azure_srednje_edu_connection import engine
from sqlalchemy.types import NVARCHAR, INTEGER, NUMERIC
from azure_data_cleaning_and_preparation import founderCategory, county, city, town, lecture_lang, school_type, school_name, edu_program, class_facttable, school_facttable, manual_corr_skola_fct, yn_school_info, yn_class_info


def upl_founderCategory(osnivac=founderCategory()):
   osnivac.to_sql(name="kategorija_osnivaca"
                , con=engine
                , if_exists='append'
                , index=False
                , dtype={'osnivac_kategorija_id': INTEGER(),
                         'osnivac_kategorija': NVARCHAR()
                         })

if __name__ == "__main__":
   upl_founderCategory()
   
def upl_county(okrug=county()):
   okrug.to_sql(name="okrug"
              , con=engine
              , if_exists='append'
              , index=False
              , dtype={'okrug_id':  INTEGER(),
                       'okrug': NVARCHAR()
                         })

if __name__ == "__main__":
   upl_county()
   
def upl_city(opstina=city()):
   opstina.to_sql(name="opstina"
                , con=engine
                , if_exists='append'
                , index=False
                , dtype={'opstina_id':  INTEGER(),
                         'opstina': NVARCHAR()
                         })

if __name__ == "__main__":
  upl_city()
  

def upl_town(mesto=town()):
  mesto.to_sql(name="mesto"
             , con=engine
             , if_exists='append'
             , index=False
             , dtype={'mesto_id':  INTEGER(),
                         'mesto': NVARCHAR()
                         })

if __name__ == "__main__":
  upl_town()
  
def upl_lecture_lang(jezikNastave=lecture_lang()):
   jezikNastave.to_sql(name="jezik_nastave"
                     , con=engine
                     , if_exists='append'
                     , index=False
                     , dtype={'jezik_nastave_id':  INTEGER(),
                              'jezik_nastave': NVARCHAR()
                         })

if __name__ == "__main__":
  upl_lecture_lang()
  
def upl_school_type(vrstaUstanove=school_type()):
   vrstaUstanove.to_sql(name="vrsta_ustanove"
                     , con=engine
                     , if_exists='append'
                     , index=False
                     , dtype={'vrsta_ustanove_id':  INTEGER(),
                              'vrsta_ustanove': NVARCHAR()
                         })

if __name__ == "__main__":
   upl_school_type()
   
def upl_school_name(nazivUstanove=school_name()):
   nazivUstanove.to_sql(name="naziv_ustanove"
                     , con=engine
                     , if_exists='append'
                     , index=False
                     , dtype={'naziv_ustanove_id':  INTEGER(),
                              'opstina_id':  INTEGER(),
                              'naziv_ustanove': NVARCHAR()
                         })

if __name__ == "__main__":
   upl_school_name()
   
def upl_edu_program(nastavniProgram=edu_program()):
   nastavniProgram.to_sql(name="nastavni_program"
                        , con=engine
                        , if_exists="append"
                        , index=False
                        , dtype={'program_id':  INTEGER(),
                                 'nastavni_program': NVARCHAR()
                         })

if __name__ == "__main__":
   upl_edu_program()

def upl_yn_school_info(dodatno_o_skoli=yn_school_info()):
   dodatno_o_skoli.to_sql(name="dodatno_o_skoli"
                        , con=engine
                        , if_exists='append'
                        , index=False
                        , dtype={'dodatno_o_skoli_id':  INTEGER(),
                                 'dodatno_o_skoli': NVARCHAR()
                          })

if __name__ == "__main__":
  upl_yn_school_info()

def upl_yn_class_info(podaci_o_razredima=yn_class_info()):
   podaci_o_razredima.to_sql(name="podaci_o_razredima"
                           , con=engine
                           , if_exists='append'
                           , index=False
                           , dtype={'podaci_o_razredima_id':  INTEGER(),
                                    'podaci_o_razredima': NVARCHAR()
                             })

if __name__ == "__main__":
  upl_yn_class_info()
   
def upl_class_facttable(razredi=class_facttable()):
   razredi.to_sql(name="razredi"
                , con=engine
                , if_exists="append"
                , index=False
                , dtype={"row_id": INTEGER(),
                         "naziv_ustanove_id": INTEGER(),
                         "program_id": INTEGER(),
                         "jezik_nastave_id": INTEGER(),
                         "skolska_godina": NVARCHAR(),
                         "razred": NVARCHAR(),
                         "odeljenje": NVARCHAR(),
                         "broj_ucenika": NVARCHAR()
                        })

if __name__ == "__main__":
   upl_class_facttable()

def upl_school_facttable(razredi=manual_corr_skola_fct()):
   razredi.to_sql(name="skole"
                , con=engine
                , if_exists="append"
                , index=False
                , dtype={"row_id": INTEGER(),
                         "naziv_ustanove_id": INTEGER(),
                         "osnivac_kategorija_id": INTEGER(),
                         "vrsta_ustanove_id": INTEGER(),
                         "okrug_id": INTEGER(),
                         "opstina_id": INTEGER(),
                         "mesto_id": INTEGER(),
                         "ulica": NVARCHAR(),
                         "latitude": NUMERIC(13,10),
                         "longitude": NUMERIC(13,10),
                         "godina_osnivanja": NVARCHAR(),
                         "maticni_broj": NVARCHAR(),
                         "pib": NVARCHAR(),
                         "dodatno_o_skoli": NVARCHAR(),
                         "podaci_o_razredima": NVARCHAR(),
                         "povrsina_objekta": INTEGER(),
                         "broj_ucionica": INTEGER(),
                         "povrsina_ucionica": INTEGER(),
                         "broj_kuhinja": INTEGER(),
                         "povrsina_kuhinja": INTEGER(),
                         "broj_biblioteka": INTEGER(),
                         "broj_radionica": INTEGER(),
                         "povrsina_radionica": INTEGER(),
                         "broj_restorana": INTEGER(),
                         "broj_kabineta": INTEGER(),
                         "povrsina_kabineta": INTEGER(),
                         "broj_laboratorija": INTEGER(),
                         "povrsina_laboratorija": INTEGER(),
                         "broj_sala": INTEGER(),
                         "povrsina_sala": INTEGER()
                        })

if __name__ == "__main__":
   upl_school_facttable()

print("""\nData have been inserted into the tables.
Podaci su ubačeni u tabele.\n""")