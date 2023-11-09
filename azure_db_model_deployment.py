from azure_db_model import NumericAsInteger, Base, Osnivac, Okrug, Opstina, Mesto, JezikNastave, VrstaUstanove, NazivUstanove, NastavniProgram, Razred, Skola, DodatnoOSkoli, PodaciORazredima
from azure_srednje_edu_connection import engine

Base.metadata.create_all(bind=engine)

print("""\nTables have been deployed to the server.
Tabele su postavljene na server.\n""")