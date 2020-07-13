import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import zipfile
import pathlib
import tempfile
import itertools
import shutil
import json
import psycopg2
import multiprocessing as mp

from congress.etl.vars import TABLE_CREATE_BILLS
from congress.etl.base import DataToDB


class BillsToDB(DataToDB):
    """
    Object to move data from flat JSON files into MongoDB.
    Zipped data can be downloaded at
    https://www.propublica.org/datastore/dataset/congressional-data-bulk-legislation-bills

    Downloading bills is best done using the context manager as it will automatically clean
    the temporary directory created.  This can be done manually by calling BillsToDB.dir.cleanup().
    """

    def __init__(self):
        """Initialize object.
        """
        self.dir = tempfile.TemporaryDirectory()
        print(f"{self.dir.name}")

    def __enter__(self):
        """Enter

        :return: self
        :rtype: BillsToDB
        """
        return self

    def __exit__(self, *args):
        """Exit.
        """
        self.dir.cleanup()

    def fetch(self, congress):
        url = f'https://s3.amazonaws.com/pp-projects-static/congress/bills/{congress}.zip'
        r = requests.get(url, allow_redirects=True)

        fname = f'{self.dir.name}/{congress}.zip'
        with open(fname, 'wb') as f:
            f.write(r.content)

        with zipfile.ZipFile(fname, 'r') as zip_ref:
            zip_ref.extractall(self.dir.name)
        
        files = self._get_files_by_type()
        # amendments break things currently
        files = [i for i in files if 'amendments' not in i]

        return pd.DataFrame(files, columns=['file'])

    def cleanse(self, d):
        """Extract data from flat files and transform for database.

        :param f: /path/to/json
        :type f: str
        :return: row for database
        :rtype: list
        """
        f = d['file']
        try:
            d = json.load(open(f, "r"))
        except:
            print(f'error with {f}')
            return False
        try:
            del d['_id']
        except KeyError:
            pass
        if 'bill_body' not in d:
            d['bill_body'] = 'NO BODY'
        if d['introduced_at'] is None:
            d['inroduced_at'] = '1900-01-01'
        vals = []
        for v in ('actions', 'amendments', 'bill_id', 'bill_type',
                  'committees', 'congress', 'cosponsors', 'enacted_as',
                  'history', 'introduced_at', 'number', 'official_title',
                  'popular_title', 'related_bills',
                  'short_title', 'sponsor', 'status', 'status_at', 'subjects',
                  'subjects_top_term', 'summary',
                  'titles', 'updated_at', 'bill_body'):
            if v not in d:
                print(f'error with {v}')
                vals.append(None)
            else:
                if isinstance(d[v], list) or isinstance(d[v], dict):
                    vals.append(json.dumps(d[v]))
                else:
                    vals.append(d[v])

        return vals

    def _get_files_by_type(self, typ=".json"):
        """Get all types of a certain file.

        :param ftype: file extension, defaults to ".json"
        :type ftype: str, optional
        :return: /path/to/files
        :rtype: list
        """
        found = []
        for root, _, files in os.walk(self.dir.name):
            for file in files:
                if file.endswith(typ):
                    found.append(os.path.join(root, file))
        return found

    def create_table(self):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database='congress',
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.execute(TABLE_CREATE_BILLS)
        conn.commit()
        conn.close()

    def to_psql(self, v):
        """Execute insert statement(s).

        :param v: tuples of data
        :type v: list
        """
        query =  '''
        INSERT INTO bills
            (actions, amendments, bill_id, bill_type, committees, congress, cosponsors, enacted_as,
             history, introduced_at, number, official_title, popular_title, related_bills,
             short_title, sponsor, status, status_at, subjects, subjects_top_term, summary,
             titles, updated_at, bill_body)
        VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT ON CONSTRAINT bills_pkey DO NOTHING
        '''
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database='congress',
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT', '5432')
        )
        cursor = conn.cursor()
        cursor.executemany(query, v)
        conn.commit()
        conn.close()

    # def add_bill_bodies(self):
    #     """Scrape bill bodies.
    #     """
    #     conn = psycopg2.connect(
    #         user=os.environ.get('PSQL_USER'),
    #         password=os.environ.get('PSQL_PASS'),
    #         database='congress',
    #         host=os.environ.get('PSQL_HOST'),
    #         port=os.environ.get('PSQL_PORT', '5432')
    #     )
    #     cursor = conn.cursor()
    #     cursor.execute(
    #         f"""SELECT 
    #                 bill_type, bill_id, congress 
    #             FROM 
    #                 {self.table} 
    #             WHERE 
    #                 (bill_body = 'NO BODY' OR bill_body IS NULL) 
    #                  AND congress = {self.congress}
    #         """
    #     )
    #     df = pd.DataFrame(
    #         cursor.fetchall(), columns=['bill_type', 'bill_id', 'congress']
    #     )
    #     conn.close()

    #     for _, row in df.to_dict(orient='records'):
    #         body = self.fetch_bill_text(row['bill_type'], row['bill_id'], row['congress'])
    #         if body is not None:
    #             conn = psycopg2.connect(
    #                 user=os.environ.get('PSQL_USER'),
    #                 password=os.environ.get('PSQL_PASS'),
    #                 database=self.database,
    #                 host=os.environ.get('PSQL_HOST'),
    #                 port=os.environ.get('PSQL_PORT', '5432')
    #             )
    #             cursor = conn.cursor()
    #             command = f"""
    #                 UPDATE 
    #                     {self.table}
    #                 SET 
    #                     bill_body = '{body}' 
    #                 WHERE 
    #                     bill_type = '{row['bill_type']}' 
    #                     AND bill_id = '{row['bill_id']}' 
    #                     AND congress = {row['congress']}
    #             """
    #             cursor.execute(command)
    #             conn.commit()
    #             conn.close()

    # @staticmethod
    # def fetch_bill_text(bill_type, bill_id, congress):
    #     """Get text of bill.

    #     :param bill_type: type of bill (see mapper variable)
    #     :type bill_type: str
    #     :param bill_id: unique id for a bill
    #     :type bill_id: str
    #     :param congress: congress number
    #     :type congress: int or str
    #     :return: text of bill
    #     :rtype: str
    #     """
    #     mapper = {
    #         "hconres": "house-concurrent-resolution",
    #         "hjres": "house-joint-resolution",
    #         "hr": "house-bill",
    #         "hres": "house-resolution",
    #         "s": "senate-bill",
    #         "sconres": "senate-concurrent-resolution",
    #         "sjres": "senate-joint-resolution",
    #         "sres": "senate-resolution"
    #     }
    #     if bill_type not in mapper:
    #         return None

    #     bill_num = bill_id.split("-")[0]
    #     bill_num = "".join([i for i in bill_num if i.isdigit()])

    #     mapped_btype = mapper[bill_type]
    #     ord_cong = BillsToDB.ordinal(int(congress))
    #     url = f"https://www.congress.gov/bill/{ord_cong}-congress/{mapped_btype}/{bill_num}/text?format=txt"

    #     try:
    #         p = BeautifulSoup(requests.get(url).text)
    #         body = p.find(id="billTextContainer").get_text()
    #     except AttributeError:
    #         print(bill_type, bill_id, congress, url)
    #         return None
    #     return body

    # @staticmethod
    # def ordinal(n):
    #     """Transform number to have suffix (1 -> 1st, ...)

    #     :param n: number to transform
    #     :type n: int
    #     :return: number with suffix
    #     :rtype: str
    #     """
    #     return "%d%s" % (n, "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])
