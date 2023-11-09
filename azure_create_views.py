from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from azure_srednje_edu_connection import engine

def create_view(engine, view_name, sql):

    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        session.execute(text(sql))
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error creating indexed view {view_name}: {str(e)}")
    finally:
        session.close()

def index_view(engine, index_table_name, column):
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    sql = f"CREATE UNIQUE CLUSTERED INDEX IDX_{index_table_name} ON dbo.{index_table_name} ({column});"
    
    try:
        session.execute(text(sql))
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error creating indexed view {index_table_name}: {str(e)}")
    finally:
        session.close()

fst_pg_map_view = """
CREATE VIEW fst_pg_map_view WITH SCHEMABINDING AS 
SELECT skole.naziv_ustanove_id
     , skole.ulica
	 , skole.godina_osnivanja
	 , skole.latitude
	 , skole.longitude
	 , naziv_ustanove.naziv_ustanove
	 , kategorija_osnivaca.osnivac_kategorija
	 , vrsta_ustanove.vrsta_ustanove
	 , COUNT_BIG(*) as tmp
FROM dbo.skole 
JOIN dbo.naziv_ustanove ON dbo.naziv_ustanove.naziv_ustanove_id = dbo.skole.naziv_ustanove_id 
JOIN dbo.kategorija_osnivaca ON dbo.kategorija_osnivaca.osnivac_kategorija_id = dbo.skole.osnivac_kategorija_id 
JOIN dbo.vrsta_ustanove ON dbo.vrsta_ustanove.vrsta_ustanove_id = dbo.skole.vrsta_ustanove_id 
GROUP BY skole.naziv_ustanove_id
       , skole.ulica
	   , skole.godina_osnivanja
	   , skole.latitude
	   , skole.longitude
	   , naziv_ustanove.naziv_ustanove
	   , kategorija_osnivaca.osnivac_kategorija
	   , vrsta_ustanove.vrsta_ustanove;

"""

fst_pg_t1_view = """
CREATE VIEW fst_pg_t1_view WITH SCHEMABINDING AS 
SELECT skole.naziv_ustanove_id
     , skole.ulica
	 , skole.godina_osnivanja
	 , skole.maticni_broj
	 , skole.pib
	 , dodatno_o_skoli.dodatno_o_skoli
	 , podaci_o_razredima.podaci_o_razredima
	 , naziv_ustanove.naziv_ustanove
	 , kategorija_osnivaca.osnivac_kategorija
	 , vrsta_ustanove.vrsta_ustanove
	 , mesto.mesto
	 , okrug.okrug
	 , opstina.opstina
	 , COUNT_BIG(*) as tmp
FROM dbo.skole 
JOIN dbo.dodatno_o_skoli ON dbo.dodatno_o_skoli.dodatno_o_skoli_id = dbo.skole.dodatno_o_skoli_id 
JOIN dbo.podaci_o_razredima ON dbo.podaci_o_razredima.podaci_o_razredima_id = dbo.skole.podaci_o_razredima_id 
JOIN dbo.naziv_ustanove ON dbo.naziv_ustanove.naziv_ustanove_id = dbo.skole.naziv_ustanove_id 
JOIN dbo.kategorija_osnivaca ON dbo.kategorija_osnivaca.osnivac_kategorija_id = dbo.skole.osnivac_kategorija_id 
JOIN dbo.vrsta_ustanove ON dbo.vrsta_ustanove.vrsta_ustanove_id = dbo.skole.vrsta_ustanove_id 
JOIN dbo.mesto ON dbo.mesto.mesto_id = dbo.skole.mesto_id 
JOIN dbo.okrug ON dbo.okrug.okrug_id = dbo.skole.okrug_id 
JOIN dbo.opstina ON dbo.skole.opstina_id = dbo.opstina.opstina_id
GROUP BY skole.naziv_ustanove_id
	   , skole.ulica
	   , skole.godina_osnivanja
	   , skole.maticni_broj
	   , skole.pib
	   , dodatno_o_skoli.dodatno_o_skoli
	   , podaci_o_razredima.podaci_o_razredima
	   , naziv_ustanove.naziv_ustanove
	   , kategorija_osnivaca.osnivac_kategorija
	   , vrsta_ustanove.vrsta_ustanove
	   , mesto.mesto
	   , okrug.okrug
	   , opstina.opstina;

"""

fst_pg_t3_view = """
CREATE VIEW fst_pg_t3_view WITH SCHEMABINDING AS 
SELECT razredi.row_id
	 , razredi.naziv_ustanove_id
     , razredi.skolska_godina
     , razredi.razred
     , razredi.odeljenje
     , razredi.broj_ucenika
     , nastavni_program.nastavni_program
     , skole.godina_osnivanja
     , COUNT_BIG(*) as tmp
FROM dbo.razredi 
JOIN dbo.nastavni_program ON dbo.nastavni_program.program_id = dbo.razredi.program_id 
JOIN dbo.skole ON dbo.razredi.naziv_ustanove_id = dbo.skole.naziv_ustanove_id
GROUP BY razredi.row_id
	   , razredi.naziv_ustanove_id
       , razredi.skolska_godina
       , razredi.razred
       , razredi.odeljenje
       , razredi.broj_ucenika
       , nastavni_program.nastavni_program
       , skole.godina_osnivanja;

"""

view_dict = {"fst_pg_map_view": [fst_pg_map_view, "naziv_ustanove_id"]
           , "fst_pg_t1_view": [fst_pg_t1_view, "naziv_ustanove_id"]
           , "fst_pg_t3_view": [fst_pg_t3_view, "row_id"]
}

for k, v in view_dict.items():
    create_view(engine, k, v[0])
    index_view(engine, k, v[1])



