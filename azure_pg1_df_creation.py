import pandas as pd
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.pool import NullPool
import os

connection_string = os.environ["AZURE_SQL_SREDNJE_EDU"]
db_type = "mssql+pyodbc"

def local_conn(conn_str=connection_string, db_type=db_type):
    conn_url = URL.create(db_type, query={"odbc_connect": conn_str})
    engine = create_engine(conn_url, poolclass=NullPool)
    return engine

def createSession(engine):
    Session = sessionmaker(bind=engine)
    return Session()

def createMetaData(engine):
    metadata_obj = db.MetaData()
    metadata_obj.create_all(bind=engine)
    metadata_obj.reflect(bind=engine)
    return metadata_obj

def createTable(name, source_name, metadata_obj, engine):
    table = db.Table(source_name, metadata_obj, autoload_with=engine)
    return table

def createMultiTable(engine, metadata_obj, table_scope):
    table_list = []
    for table_name, table_source in table_scope.items():
        table = createTable(table_name, table_source, metadata_obj, engine)
        table_list.append(table)
    return table_list

def map_query(session, Map):
    return session.query(Map.c.naziv_ustanove_id, Map.c.ulica, Map.c.godina_osnivanja, Map.c.latitude, Map.c.longitude,
                          Map.c.naziv_ustanove, Map.c.osnivac_kategorija, Map.c.vrsta_ustanove)

def osn_info_query(session, T1):
    return session.query(T1.c.naziv_ustanove_id, T1.c.ulica, T1.c.godina_osnivanja, T1.c.maticni_broj, T1.c.pib, T1.c.dodatno_o_skoli\
                             , T1.c.podaci_o_razredima, T1.c.naziv_ustanove, T1.c.osnivac_kategorija, T1.c.vrsta_ustanove, T1.c.mesto, T1.c.okrug, T1.c.opstina)
                             
def dod_info_query(session, T2):
    return session.query(T2.c.povrsina_objekta, T2.c.broj_ucionica, T2.c.povrsina_ucionica, T2.c.broj_kuhinja, T2.c.povrsina_kuhinja, T2.c.broj_biblioteka, T2.c.broj_radionica\
                             , T2.c.povrsina_radionica, T2.c.broj_restorana, T2.c.broj_kabineta, T2.c.povrsina_kabineta, T2.c.broj_laboratorija, T2.c.povrsina_laboratorija, T2.c.broj_sala\
                             , T2.c.povrsina_sala, T2.c.godina_osnivanja, T2.c.naziv_ustanove_id)

def razredi_query(session, T3):
    return session.query(T3.c.naziv_ustanove_id, T3.c.skolska_godina, T3.c.razred, T3.c.odeljenje\
                            , T3.c.broj_ucenika, T3.c.nastavni_program, T3.c.godina_osnivanja)

def createDF(name_df, sql, engine):
    name_df = pd.read_sql(sql=sql, con=engine)
    return name_df

def createMultiDF(query_list, engine):
    df_list = []
    for name_df, query in query_list.items():
        df = createDF(name_df, query.statement, engine)
        df_list.append(df)
    return df_list

def pg1_df_creation():
    engine = local_conn()
    session = createSession(engine)
    metadata_obj = createMetaData(engine)

    table_scope = {"Map": "fst_pg_map_view", "T1": "fst_pg_t1_view", "T2": "skole", "T3": "fst_pg_t3_view"}
    tables = createMultiTable(engine, metadata_obj, table_scope)
    Map, T1, T2, T3 = tables

    # Perform queries
    queries = {
        "map_df": map_query(session, Map),
        "osn_info_df": osn_info_query(session, T1),
        "dod_info_df": dod_info_query(session, T2),
        "razredi_df": razredi_query(session, T3)
    }

    dfs = createMultiDF(queries, engine)

    return dfs

if __name__ == "__main__":
    pg1_df_creation()