from itertools import product
import random

import pandas as pd
from sklearn import linear_model
from sklearn.metrics import mean_squared_error

from models.db_models import session, ZillowMetrics


class Model(object):
    def __init__(self, sklearn_model):
        self.sklearn_model = sklearn_model

    def _create_dummy_cols(self, dataframe, dummy_cols):
        return pd.concat(
            [dataframe, pd.concat([pd.get_dummies(df[col], prefix=col) for col in df[dummy_cols]], axis=1)], axis=1)

    def _split_data(self, dataframe, training_split=.8):
        training_data = dataframe[:int(len(dataframe) * training_split)]
        test_data = dataframe[len(training_data):]
        assert len(training_data) + len(test_data) == len(dataframe)
        return training_data, test_data

    def add_data(self, dataframe, y_col, dummy_cols=None, training_split=.8):

        self.dataframe = dataframe.dropna(axis=0)
        self.y_col = y_col
        self.dummy_cols = dummy_cols
        self.training_split = training_split

        if self.dummy_cols:
            self.dataframe = self._create_dummy_cols(self.dataframe, self.dummy_cols).drop(dummy_cols, axis=1)

        self.training_df, self.testing_df = self._split_data(self.dataframe, self.training_split)
        self.x_train, self.x_test = self._split_data(self.dataframe.drop(y_col, axis=1), self.training_split)
        self.y_train, self.y_test = self._split_data(self.dataframe[self.y_col], self.training_split)

    def model_data(self, print_summary=True):
        self.sklearn_model.fit(self.x_train, self.y_train)
        self.y_predicted = self.sklearn_model.predict(self.x_test)
        self.testing_df['y_predicted'] = self.y_predicted
        self.RMSE = round(mean_squared_error(self.y_test, self.y_predicted) ** 0.5, 2)
        self.coefficients = zip(self.x_train.columns, [round(coef, 2) for coef in self.sklearn_model.coef_[0]])
        self.score = round(self.sklearn_model.score(self.x_test, self.y_test), 2)
        self.model_name = self.sklearn_model.__class__.__name__

        if print_summary:
            print '=========================================================='
            print 'Model: {}'.format(self.model_name)
            print 'Score: {}'.format(self.score)
            print 'RMSE: {}'.format(self.RMSE)
            print 'Coeficients:\n{}'.format(self.coefficients)
            print 'Intercept: {}'.format(round(self.sklearn_model.intercept_, 2))
            print '=========================================================='


def generate_column_shifts(labels, shift_range):
    return list(product(*[[p for p in product(shift_range, [label])] for label in labels]))


def find_best_model(column_shifts, df, y_col, model):
    results = []
    for column_shift in column_shifts:
        model_df = df.copy()
        for column in column_shift:
            model_df[column[1]] = model_df[column[1]].shift(column[0])
        model.add_data(model_df, y_col=[y_col])
        model.model_data()
        results.append({'shift': column_shift,
                        'RMSE': model.RMSE,
                        'Score': model.score,
                        'Coefs': model.coefficients})

    return max(results, key=lambda x: x['Score'])


# yelp_df = pd.DataFrame([row.as_dict() for row in session.query(Yelp).all()])
# yelp_df = yelp_df.rename(columns={'date_published': 'month'})
zillow_results = session.query(ZillowMetrics).filter(ZillowMetrics.ZRI != None, ZillowMetrics.ZHVI != None).all()

zillow_metrics_df = pd.DataFrame([row.as_dict() for row in zillow_results])
# merged_df = yelp_df.merge(zillow_metrics_df, on=['month', 'zip_code'])

lin_model = Model(linear_model.LinearRegression())

min_periods = 8
lag_columns = ['ZRI']

zip_code_sample = zillow_metrics_df.zip_code.unique()
random.shuffle(zip_code_sample)
zip_code_sample = zip_code_sample[:10]

best_models = []
for zip_code in zip_code_sample:
    model_df = zillow_metrics_df[['median_sales_price', 'ZRI', 'month']][zillow_metrics_df['zip_code'] ==
                                                                         zip_code].sort('month')
    max_lag = len(model_df) - min_periods
    column_shifts = generate_column_shifts(lag_columns, range(max_lag))
    best_model = find_best_model(column_shifts, model_df[['median_sales_price', 'ZRI']], 'median_sales_price',
                                 lin_model)
    best_model['zip_code'] = zip_code
    best_models.append(best_model)
