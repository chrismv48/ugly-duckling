"""Script to get yelp data from their API"""
import datetime

from sqlalchemy import extract
from yelpapi import YelpAPI

from models.db_models import YelpAPIDb, session, ZipCode
from config import login_data, LOGGER


def is_business_valid(business, zip_code):
    if business['location'].has_key('postal_code'):
        if business['location']['postal_code'] == zip_code:
            return True

    return False


def main():
    yelp_api = YelpAPI(login_data['yelp_consumer_key'],
                       login_data['yelp_consumer_secret'],
                       login_data['yelp_token'],
                       login_data['yelp_token_secret'])

    zip_codes = [row.zip_code for row in session.query(ZipCode).all()]

    current_month = datetime.date.today().month
    current_rows = session.query(YelpAPIDb).filter(extract('month', YelpAPIDb.date_created) == current_month).all()
    current_rows = [row.as_dict() for row in current_rows]
    existing_zip_codes = [row['zip_code'] for row in current_rows]
    remaining_zip_codes = [zip_code for zip_code in zip_codes if zip_code not in existing_zip_codes]

    category_list = ["cafes",
                     "newamerican",
                     "indpak",
                     "italian",
                     "japanese",
                     "thai"]

    for i, zip_code in enumerate(remaining_zip_codes):
        zip_code_results = []
        for category in category_list:
            offset = 0
            total_count = 21
            results_per_query_limit = 20
            business_counter = 1
            remaining_count = 1

            LOGGER.info("Extracting {} restaurants from zip code {} ({} out of {})".format(category, zip_code, i,
                                                                                           len(remaining_zip_codes)))
            while remaining_count > 0:
                try:

                    search_results = yelp_api.search_query(location=zip_code,
                                                           category_filter=category, sort=0, limit=20,
                                                           offset=offset)
                    total_count = search_results['total']
                except YelpAPI.YelpAPIError as e:
                    print e
                    break
                if search_results['total'] == 0:
                    session.merge(YelpAPIDb(zip_code=zip_code, date_created=datetime.date.today(), avg_rating=None,
                                            business_count=0))
                    session.commit()
                    break
                for business in search_results['businesses']:
                    if is_business_valid(business, zip_code):
                        print "{} out of {} businesses".format(business_counter, total_count)
                        zip_code_results.append({"zip_code": zip_code,
                                                 "rating": business['rating'],
                                                 "review_count": business["review_count"]})
                    business_counter += 1

                remaining_count = total_count - business_counter
                offset += results_per_query_limit

        if zip_code_results:
            total_review_count = sum([business['review_count'] for business in zip_code_results])
            zip_code_avg_rating = sum(
                [business['rating'] * business['review_count'] for business in zip_code_results]) / total_review_count
            row = YelpAPIDb(zip_code=zip_code, date_created=datetime.date.today(), avg_rating=zip_code_avg_rating,
                            business_count=len(zip_code_results))
            session.merge(row)
            session.commit()
        else:
            session.merge(
                YelpAPIDb(zip_code=zip_code, date_created=datetime.date.today(), avg_rating=None, business_count=0))
            session.commit()
    session.close()


if __name__ == '__main__':
    main()
