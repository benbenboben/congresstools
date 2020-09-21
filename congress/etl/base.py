import psycopg2
import os
import multiprocessing as mp
import requests


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
        return [i for i in vals if bool(i)]

    def to_psql(self, df):
        raise NotImplementedError

    def create_table(self):
        raise NotImplementedError

    def run_pipeline(self, *args, **kwargs):
        self.create_table()
        df = self.fetch(*args, **kwargs)
        vals = self.prep_for_psql(df)
        self.to_psql(vals)

    def _base_call(self, endpoint, headers=None, params=None):
        """Hit endpoint.

        :param endpoint: url
        :type endpoint: str
        :param headers: headers for request, defaults to None
        :type headers: dict, optional
        :param params: parameters for API, defaults to None
        :type params: dict, optional
        :return: response   
        :rtype: dict
        """
        if headers is None:
            headers = {"X-API-Key": os.environ['PRO_PUBLICA_API_KEY']}
        else:
            headers = {**headers, **{"X-API-Key": os.environ['PRO_PUBLICA_API_KEY']}}

        params = {} if params is None else params

        r = requests.get(endpoint, headers=headers, params=params)
        if r.status_code != 200:
            raise APIError(f"Error fetching member data -- received {r.status_code}")
        else:
            return r.json()





