import datetime

from sqlalchemy import Column, String, Integer, Float, func, create_engine, Date, DateTime, extract
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, validates

from models.base import SerializedModel

DB_LOCATION = "postgresql+psycopg2://carmstrong@localhost/ugly_duckling"
Base = declarative_base()
engine = create_engine(DB_LOCATION)
Session = sessionmaker(bind=engine)
session = Session()


def create_tables():
    Base.metadata.create_all(bind=engine)


class Yelp(Base, SerializedModel):
    __table_args__ = {'schema': 'ugly_duckling'}
    __bind_key__ = 'ugly_duckling'
    __tablename__ = "yelp"

    zip_code = Column(String(5), primary_key=True)
    date_published = Column(String(10), primary_key=True)
    num_reviews = Column(Integer)
    review_rating = Column(Float)


class Indeed(Base, SerializedModel):
    __table_args__ = {'schema': 'ugly_duckling'}
    __bind_key__ = 'ugly_duckling'
    __tablename__ = "indeed"

    zip_code = Column(String(5), primary_key=True)
    date_created = Column(Date, default=datetime.date.today(), primary_key=True)
    job_count = Column(Integer)
    timestamp = Column(DateTime, default=func.now())

    def month(self):
        return extract('month', self.date_created)


class YelpAPIDb(Base, SerializedModel):
    __table_args__ = {'schema': 'ugly_duckling'}
    __bind_key__ = 'ugly_duckling'
    __tablename__ = "yelp_api"

    zip_code = Column(String(5), primary_key=True)
    date_created = Column(Date, default=datetime.date.today(), primary_key=True)
    business_count = Column(Integer)
    avg_rating = Column(Float(precision=2))
    timestamp = Column(DateTime, default=func.now())

    def month(self):
        return extract('month', self.date_created)


class ZipCode(Base, SerializedModel):
    __table_args__ = {'schema': 'ugly_duckling'}
    __bind_key__ = 'ugly_duckling'
    __tablename__ = "zip_code"

    zip_code = Column(String(5), primary_key=True)
    city = Column(String)
    state = Column(String(2))
    metro = Column(String)
    county = Column(String)

    @validates('zip_code')
    def add_leading_zero(self, key, field):
        if len(str(field)) == 4:
            return '0' + str(field)
        else:
            return field


class ZillowMetrics(Base, SerializedModel):
    __table_args__ = {'schema': 'ugly_duckling'}
    __bind_key__ = 'ugly_duckling'
    __tablename__ = "zillow_metrics"

    zip_code = Column(String(5), primary_key=True)
    month = Column(String, primary_key=True)
    ZHVI = Column(Float)
    ZRI = Column(Float)
    median_listing_price = Column(Float)
    median_sales_price = Column(Float)

    @validates('zip_code')
    def add_leading_zero(self, key, field):
        if len(field) == 4:
            return '0' + field
        else:
            return field


class BuildingPermit(Base, SerializedModel):
    __table_args__ = {'schema': 'ugly_duckling'}
    __bind_key__ = 'ugly_duckling'
    __tablename__ = "building_permit"

    month = Column(String, primary_key=True)
    city = Column(String, primary_key=True)
    state = Column(String, primary_key=True)
    num_buildings = Column(Integer, default=0)
    num_units = Column(Integer, default=0)
    construction_cost = Column(Integer, default=0)


create_tables()
