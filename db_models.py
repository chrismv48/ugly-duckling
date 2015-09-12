from sqlalchemy import Column, String, Integer, Float, func, create_engine, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime
import os

DB_LOCATION = 'sqlite:///{}/yelp.db' .format(os.getcwd())
Base = declarative_base()
engine = create_engine(DB_LOCATION)
Session = sessionmaker(bind=engine)
session = Session()


def convert_query_results(results):
    """
    Converts results of SQLAlchemy query to a list of dicts
    """

    # The data structure of the results differs when 1 table vs >1 table is queried; this if statement checks for
    # that.
    if not results:
        return []
    if hasattr(results[0], "count"):
        merged_array = []
        for record in results:
            record_dict = {}
            for n in range(len(record)):
                if record[n]:
                    record_dict.update(record[n].__dict__)
                    record_dict.pop('_sa_instance_state')
            merged_array.append(record_dict)
        return merged_array
    else:
        return [{k: v for k, v in result_dict.__dict__.iteritems()
                 if k != '_sa_instance_state'} for result_dict in results]


def create_tables():
    Base.metadata.create_all(bind=engine)


class Yelp(Base):
    __tablename__ = "yelp"
    zip_code = Column(String(5), primary_key=True)
    date_published = Column(String(10), primary_key=True)
    num_reviews = Column(Integer)
    review_rating = Column(Float)


class Indeed(Base):
    __tablename__ = "indeed"
    zip_code = Column(String(5), primary_key=True)
    date_published = Column(Date, default=datetime.date.today(), primary_key=True)
    job_count = Column(Integer)


class YelpAPIDb(Base):
    __tablename__ = "yelp_api"
    zip_code = Column(String(5), primary_key=True)
    date_extracted = Column(Date, default=datetime.date.today(), primary_key=True)
    business_count = Column(Integer)
    avg_rating = Column(Float(precision=2))
