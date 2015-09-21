"""Docstring goes here"""
from sqlalchemy import create_engine

from models.db_models import session, Yelp, YelpAPIDb, Indeed

sqlite_db = create_engine('sqlite:///yelp.db')

with sqlite_db.connect() as conn:
    yelp_api_results = conn.execute('SELECT * FROM yelp_api').fetchall()
    yelp_results = conn.execute('SELECT * FROM yelp').fetchall()
    indeed_results = conn.execute('SELECT * FROM indeed').fetchall()

yelp_list = [{'zip_code': row.zip_code,
              'date_published': row.date_published,
              'num_reviews': row.num_reviews,
              'review_rating': row.review_rating} for row in yelp_results]

session.add_all([Yelp(**row) for row in yelp_list])
session.commit()

yelp_api_list = [{'zip_code': row.zip_code,
                  'date_created': row.date_extracted,
                  'business_count': row.business_count,
                  'avg_rating': row.avg_rating} for row in yelp_api_results]

session.add_all([YelpAPIDb(**row) for row in yelp_api_list])
session.commit()

indeed_list = [{'zip_code': row.zip_code,
                'date_created': row.date_published,
                'job_count': row.job_count} for row in indeed_results]

session.add_all([Indeed(**row) for row in indeed_list])
session.commit()
