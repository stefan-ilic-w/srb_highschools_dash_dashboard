from sqlalchemy import create_engine, func, select, ForeignKey, Text, TypeDecorator
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, registry
from sqlalchemy.types import DateTime, Integer, String, Numeric, NVARCHAR
from typing import List
from typing_extensions import Annotated
from decimal import Decimal

num_13_10 = Annotated[Decimal, 13]

class NumericAsInteger(TypeDecorator):
    '''normalize floating point return values into ints'''

    impl = Numeric(10, 0, asdecimal=False)
    cache_ok = True

    def process_result_value(self, value, dialect):
        if value is not None:
            value = int(value)
        return value

class Base(DeclarativeBase):
    pass

class Osnivac(Base):
    __tablename__= "kategorija_osnivaca"

    osnivac_kategorija_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    osnivac_kategorija: Mapped[str] = mapped_column(NVARCHAR(30))
    r_osnivac_skola: Mapped["Skola"] = relationship(back_populates="r_skola_osnivac")
    
    def __repr__(self) -> str:
        return f"Kategorija osnivača je {self.osnivacKategorija}"

class Okrug(Base):
    __tablename__= "okrug"

    okrug_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    okrug: Mapped[str] = mapped_column(NVARCHAR(60))
    r_okrug_skola: Mapped["Skola"] = relationship(back_populates="r_skola_okrug")

    def __repr__(self) -> str:
        return f"Naziv upravnog okruga je: {self.okrug}."
    
class Opstina(Base):
    __tablename__= "opstina"

    opstina_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    opstina: Mapped[str] = mapped_column(NVARCHAR(60))
    r_opstina_naziv_ustanove: Mapped["NazivUstanove"] = relationship(back_populates="r_naziv_ustanove_opstina")
    r_opstina_skola: Mapped["Skola"] = relationship(back_populates="r_skola_opstina")

    def __repr__(self) -> str:
        return f"Naziv opštine je: {self.opstina}."
    
class Mesto(Base):
    __tablename__= "mesto"

    mesto_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    mesto: Mapped[str] = mapped_column(NVARCHAR(60))
    r_mesto_skola: Mapped["Skola"] = relationship(back_populates="r_skola_mesto")
    
    def __repr__(self) -> str:
        return f"Naziv mesta je: {self.mesto}."
    
class JezikNastave(Base):
    __tablename__= "jezik_nastave"

    jezik_nastave_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    jezik_nastave: Mapped[str] = mapped_column(NVARCHAR(60), nullable = True)
    r_jezik_nastave_razred: Mapped["Razred"] = relationship(back_populates="r_razred_jezik_nastave")
    
    def __repr__(self) -> str:
        return f"Jezik nastave je: {self.jezik_nastave}."

class VrstaUstanove(Base):
    __tablename__= "vrsta_ustanove"

    vrsta_ustanove_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    vrsta_ustanove: Mapped[str] = mapped_column(NVARCHAR(60), nullable=True)
    r_vrsta_ustanove_skola: Mapped["Skola"] = relationship(back_populates="r_skola_vrsta_ustanove")
    
    def __repr__(self) -> str:
        return f"Ustanova je: {self.vrsta_ustanove}."

class NazivUstanove(Base):
    __tablename__ = "naziv_ustanove"

    naziv_ustanove_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    opstina_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("opstina.opstina_id"), nullable=False)
    naziv_ustanove: Mapped[str] = mapped_column(NVARCHAR(200))
    r_naziv_ustanove_skola: Mapped["Skola"] = relationship(back_populates="r_skola_naziv_ustanove")
    r_naziv_ustanove_razred: Mapped["Razred"] = relationship(back_populates="r_razred_naziv_ustanove")
    r_naziv_ustanove_opstina: Mapped["Opstina"] = relationship(back_populates="r_opstina_naziv_ustanove")
    
    def __repr__(self) -> str:
        return f"Naziv ustanove je: {self.naziv_ustanove} ({self.opstina.opstina})."

class NastavniProgram(Base):
    __tablename__= "nastavni_program"

    program_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    nastavni_program: Mapped[str] = mapped_column(NVARCHAR(410))
    r_nastavni_program_razred: Mapped["Razred"] = relationship(back_populates="r_razred_nastavni_program")

    def __repr__(self) -> str:
        return f"Nastavni program / smer je: {self.nastavni_program}."

class Razred(Base):
    __tablename__ = "razredi"

    row_id: Mapped[int] = mapped_column(Integer(), primary_key=True, nullable=False, autoincrement=False)
    naziv_ustanove_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("naziv_ustanove.naziv_ustanove_id"), nullable=False)
    program_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("nastavni_program.program_id"), nullable=False)
    jezik_nastave_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("jezik_nastave.jezik_nastave_id"), nullable=False)
    skolska_godina: Mapped[str] = mapped_column(NVARCHAR(20))
    razred: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"))
    odeljenje: Mapped[str] = mapped_column(NVARCHAR(220))
    broj_ucenika: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"))
    r_razred_naziv_ustanove: Mapped["NazivUstanove"] = relationship(back_populates="r_naziv_ustanove_razred")
    r_razred_nastavni_program: Mapped["NastavniProgram"] = relationship(back_populates="r_nastavni_program_razred")
    r_razred_jezik_nastave: Mapped["JezikNastave"] = relationship(back_populates="r_jezik_nastave_razred")
    r_razred_skola: Mapped["Skola"] = relationship(back_populates="r_skola_razred")

    def __repr__(self) -> str:
        return f"{self.nastavni_program.nastavni_program} u ustanovi {self.naziv_ustanove.naziv_ustanove} na jeziku {self.naziv_jezika.naziv_jezika} u školskoj godini {self.skolska_godina} u odeljenju {self.odeljenje} pohađalo je {self.broj_ucenika} učenika."
        
class DodatnoOSkoli(Base):
    __tablename__ = "dodatno_o_skoli"
    
    dodatno_o_skoli_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    dodatno_o_skoli: Mapped[str] = mapped_column(NVARCHAR(2), nullable=False)
    r_dodatno_o_skoli_skola: Mapped["Skola"] = relationship(back_populates="r_skola_dodatno_o_skoli")
    
class PodaciORazredima(Base):
    __tablename__ = "podaci_o_razredima"
    
    podaci_o_razredima_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), primary_key=True, nullable=False, autoincrement=False)
    podaci_o_razredima: Mapped[str] = mapped_column(NVARCHAR(2), nullable=False)
    r_podaci_o_razredima_skola: Mapped["Skola"] = relationship(back_populates="r_skola_podaci_o_razredima")

class Skola(Base):
    __tablename__ = "skole"

    row_id: Mapped[int] = mapped_column(Integer(), primary_key=True, nullable=False, autoincrement=False)
    naziv_ustanove_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("naziv_ustanove.naziv_ustanove_id"), nullable=False)
    osnivac_kategorija_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("kategorija_osnivaca.osnivac_kategorija_id"), nullable=False)
    vrsta_ustanove_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("vrsta_ustanove.vrsta_ustanove_id"), nullable=False)
    okrug_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("okrug.okrug_id"), nullable=False)
    opstina_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("opstina.opstina_id"), nullable=False)
    mesto_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("mesto.mesto_id"), nullable=False)
    ulica: Mapped[str] = mapped_column(NVARCHAR(60), nullable=True)
    latitude: Mapped[num_13_10] = mapped_column(Numeric(13,10), nullable=True)
    longitude: Mapped[num_13_10] = mapped_column(Numeric(13,10), nullable=True)
    godina_osnivanja: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    maticni_broj: Mapped[str] = mapped_column(NVARCHAR(20), nullable=True)
    pib: Mapped[str] = mapped_column(NVARCHAR(22), nullable=True)
    dodatno_o_skoli_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("dodatno_o_skoli.dodatno_o_skoli_id"), nullable=True)
    podaci_o_razredima_id: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), ForeignKey("podaci_o_razredima.podaci_o_razredima_id"), nullable=True)
    povrsina_objekta: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    broj_ucionica: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    povrsina_ucionica: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    broj_kuhinja: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    povrsina_kuhinja: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    broj_biblioteka: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    broj_radionica: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    povrsina_radionica: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    broj_restorana: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    broj_kabineta: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    povrsina_kabineta: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    broj_laboratorija: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    povrsina_laboratorija: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    broj_sala: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    povrsina_sala: Mapped[int] = mapped_column(Integer().with_variant(NumericAsInteger, "mssql"), nullable=True)
    r_skola_naziv_ustanove: Mapped["NazivUstanove"] = relationship(back_populates="r_naziv_ustanove_skola")
    r_skola_osnivac: Mapped["Osnivac"] = relationship(back_populates="r_osnivac_skola")
    r_skola_vrsta_ustanove: Mapped["VrstaUstanove"] = relationship(back_populates="r_vrsta_ustanove_skola")
    r_skola_okrug: Mapped["Okrug"] = relationship(back_populates="r_okrug_skola")
    r_skola_opstina: Mapped["Opstina"] = relationship(back_populates="r_opstina_skola")
    r_skola_mesto: Mapped["Mesto"] = relationship(back_populates="r_mesto_skola")
    r_skola_razred: Mapped["Razred"] = relationship(back_populates="r_razred_skola")
    r_skola_dodatno_o_skoli: Mapped["DodatnoOSkoli"] = relationship(back_populates="r_dodatno_o_skoli_skola")
    r_skola_podaci_o_razredima: Mapped["PodaciORazredima"] = relationship(back_populates="r_podaci_o_razredima_skola")

    def __repr__(self) -> str:
        return f"{self.naziv_ustanove.naziv_ustanove} u opštini {self.opstina.opstina} (matični broj {self.maticni_broj}) je osnovana {self.godina_osnivanja} godine."

print("""\nTables have been defined.
Tabele su definisane.\n""")
    