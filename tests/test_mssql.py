from urllib.parse import quote_plus
from dataclasses import dataclass

from sqlalchemy import create_engine, Column, Integer, String, literal_column
from sqlalchemy.future import select
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base

from geoalchemy2.types import Geography
from geoalchemy2.shape import to_shape
from geoalchemy2.elements import WKTElement

from tests.config import db_string

conn_str = f"mssql+pyodbc:///?odbc_connect={quote_plus(db_string)}"
engine = create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600, pool_size=20, future=True, echo=True)
db_session = scoped_session(sessionmaker(engine, future=True, expire_on_commit=False))
db_session.connection().connection.add_output_converter(151, str)

Base = declarative_base()

@dataclass
class Lake(Base):
    __tablename__ = "lake"
    id = Column(Integer, primary_key=True)
    name: str = Column(String)
    geom: str = Column(Geography(spatial_index=False, srid=4326))


try:
    Lake.__table__.drop(engine)
except:
    pass

Lake.__table__.create(engine)

db_session.add_all(
    [
        Lake(name="Majeur", geom="POLYGON((0 0,1 0,1 1,0 1,0 0))"),
        Lake(name="Garde", geom="POLYGON((1 0,3 0,3 2,1 2,1 0))"),
        Lake(name="Orta", geom="POLYGON((3 0,6 0,6 3,3 3,3 0))"),
    ]
)


db_session.commit()

query = db_session.scalars(select(Lake)).all()
for lake in query:
    print(lake)


# query = db_session.scalars(select(Lake).filter(func.STContains(Lake.geom, 'POINT(4 1)'))).all()
# query = db_session.scalars(select(Lake).filter(text("[geom].STContains(geometry::STGeomFromText('POINT(4 1)', 146)) = 1"))).all()   # this works!!!
# query = db_session.scalars(select(Lake).filter(Lake.geom.contains('POINT(4 1)'))).all()
query = db_session.scalars(select(Lake).filter(Lake.geom.contains('POINT(4 1)'))).all()

for lake in query:
    # test_shape = to_shape(WKTElement(lake.geom))
    print(lake.name, lake.geom)

dist = literal_column(Lake.geom.distance('POINT(4 1)'))

query = db_session.scalars(select(Lake).order_by(dist)).all()

for lake in query:
    print(lake)