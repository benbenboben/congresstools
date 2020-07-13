import psycopg2
import os
import multiprocessing as mp


class APIError(Exception):

    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = "API Error raised"

    def __str__(self):
        print(self.message)


class DataToDB(object):

    def __init__(self):
        """Initialize object.
        """
        pass

    def fetch(self):
        raise NotImplementedError

    def cleanse(self, row):
        raise NotImplementedError

    def prep_for_psql(self, df):
        vals = []
        with mp.Pool() as pool:
            vals = pool.map(self.cleanse, df.to_dict(orient='records'))
        # for row in df.to_dict(orient='records'):
        #     vals.append(self.cleanse(row))
        return vals

    def to_psql(self, df):
        raise NotImplementedError

    def create_table(self):
        raise NotImplementedError

    def run_pipeline(self, *args, **kwargs):
        self.create_table()
        df = self.fetch(*args, **kwargs)
        vals = self.prep_for_psql(df)
        self.to_psql(vals)





