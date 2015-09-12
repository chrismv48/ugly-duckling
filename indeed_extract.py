"""extracts the number of jobs posted per zip code using the Indeed API"""

from indeed import IndeedClient
import pandas as pd
import datetime
from sqlalchemy import extract
from db_models import Indeed, session, convert_query_results


def get_num_job_postings(zip_code, params=None):
    """Retrives the number of job postings for a zip_code, passing in additional optional parameters:
    {
    'q': "",       # query
    "as_phr": "",  # exact phrase,
    "as_any": "",  # at least one of these words
    "as_not": "",  # none of these words
    "as_ttl": "",  # all these words,
    "as_cmp": "",  # company name,
    "jt": "all",   # job type,
    "radius": 0,   # distance from location
    "fromage": 7,  # last 7 days
    "salary": "",  # salary range
    'l': '',       # location
    'userip': "1.2.3.4",
    'useragent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2)"
    }
    """

    search_params = {
        'q': "",  # query
        "as_phr": "",  # exact phrase,
        "as_any": "",  # at least one of these words
        "as_not": "",  # none of these words
        "as_ttl": "",  # all these words,
        "as_cmp": "",  # company name,
        "jt": "all",  # job type,
        "radius": 0,  # distance from location
        "fromage": 7,  # last 7 days
        "salary": "",  # salary range
        'l': '',  # location
        'userip': "1.2.3.4",
        'useragent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2)"
    }
    if params:
        if set(params.keys()).issubset(search_params.keys()):
            search_params.update(params)
        else:
            raise Exception("Invalid parameter keys used")

    search_params['l'] = zip_code
    search_response = client.search(**search_params)

    return search_response['totalResults']


zip_codes_df = pd.read_csv("master_zip_code_list.csv")

# add leading 0's to zip codes due to excel's stupidness
zip_codes_df['zip_code'] = zip_codes_df['zip_code'].astype(str)
zip_codes_df['zip_code'] = zip_codes_df['zip_code'].apply(lambda x: '0' * (5 - len(x)) + x)

client = IndeedClient(publisher=2777217772338190)

current_month = datetime.date.today().month
current_rows = session.query(Indeed).filter(extract('month', Indeed.date_published) == current_month).all()
current_rows = convert_query_results(current_rows)
existing_zip_codes = [row['zip_code'] for row in current_rows]
remaining_zip_codes = [zip_code for zip_code in zip_codes_df['zip_code'] if zip_code not in existing_zip_codes]

for i, zip_code in enumerate(remaining_zip_codes):

    job_count = get_num_job_postings(zip_code)
    row = Indeed(zip_code=zip_code, job_count=job_count, date_published = datetime.date.today())
    session.merge(row)
    session.commit()

    print "Scraping zip code {} ({} of {})" .format(zip_code, i, len(remaining_zip_codes))
session.close()
