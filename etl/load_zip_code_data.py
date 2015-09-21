"""Load zip code data obtained from Zillow Research"""

import requests
import pandas as pd

from config import LOGGER
from models.db_models import session, ZipCode, ZillowMetrics, engine

zhvi_data_source = {"url": "http://files.zillowstatic.com/research/public/Zip/Zip_Zhvi_AllHomes.csv",
                    "field_name": "ZHVI",
                    "filename": "Zip_ZHVI_AllHomes.csv"}
zri_data_source = {"url": "http://files.zillowstatic.com/research/public/Zip/Zip_Zri_AllHomes.csv",
                   "field_name": "ZRI",
                   "filename": "Zip_ZRI_AllHomes.csv"}
median_list_data_source = {
    "url": "http://files.zillowstatic.com/research/public/Zip/Zip_MedianListingPrice_AllHomes.csv",
    "field_name": "median_listing_price",
    "filename": "Zip_Median_Listing_Price_AllHomes.csv"}

median_sale_data_source = {"url": "http://files.zillowstatic.com/research/public/Zip/Zip_MedianSoldPrice_AllHomes.csv",
                           "field_name": "median_sales_price",
                           "filename": "Zip_Median_Sales_Price_AllHomes.csv"}

zillow_data_sources = [zhvi_data_source, zri_data_source, median_list_data_source, median_sale_data_source]


def download_zillow_csv_data(save_directory='zillow_csv_data/'):
    for data_source in zillow_data_sources:

        r = requests.get(data_source['url'], stream=True)
        with open(save_directory + data_source['filename'], 'wb') as fd:
            for chunk in r.iter_content(1024):
                fd.write(chunk)


def generate_zillow_dataframe(save_directory='zillow_csv_data/'):
    final_df = pd.DataFrame()

    for data_source in zillow_data_sources:
        df = pd.read_csv('zillow_csv_data/' + data_source['filename'])
        date_cols = filter(lambda x: '-' in x, df.columns.tolist())
        id_cols = list(set(df.columns) - set(date_cols))
        melted_df = pd.melt(df, id_vars=id_cols, value_vars=date_cols, var_name='month', value_name=data_source[
            'field_name'])
        melted_df.sort(['month', 'RegionName'], inplace=True)
        melted_df.set_index(['month', 'RegionName'], inplace=True)

        if not final_df.empty:
            final_df = final_df.combine_first(melted_df)
        else:
            final_df = melted_df.copy()

    final_df.reset_index(inplace=True)
    final_df = final_df.rename(columns={"RegionName": "zip_code",
                                        "City": "city",
                                        "Metro": "metro",
                                        "State": "state",
                                        "CountyName": 'county'})

    final_df = final_df.where((pd.notnull(final_df)), None)

    return final_df


def persist_zillow_metrics(df):
    metrics_df = df.drop(['city', 'metro', 'state', 'county'], axis=1)
    session.query(ZillowMetrics).delete()  # TODO: should append to existing data in case zillow changes something
    session.commit()
    insert_chunk = 100000
    index_start = 0
    while index_start < len(metrics_df):
        LOGGER.info('Persisting Zillow Metrics rows: {} of {}'.format(index_start + insert_chunk,
                                                                      len(metrics_df)))
        engine.execute(
            ZillowMetrics.__table__.insert(metrics_df[index_start:index_start + insert_chunk].to_dict('records')))
        index_start += insert_chunk


def persist_zip_code_data(df):
    zip_code_labels_df = df[['zip_code', 'city', 'metro', 'state', 'county']].drop_duplicates()
    session.query(ZipCode).delete()  # TODO: should append to existing data in case zillow changes something
    session.add_all([ZipCode(**row) for row in zip_code_labels_df.to_dict('records')])
    session.commit()


if __name__ == '__main__':
    download_zillow_csv_data()
    zillow_df = generate_zillow_dataframe()
    persist_zillow_metrics(zillow_df)
    persist_zip_code_data(zillow_df)
