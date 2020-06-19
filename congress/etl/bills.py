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
from pymongo import MongoClient
import json
import psycopg2

from congress.etl.vars import TABLE_CREATE_BILLS


class BillsToDB(object):
    """
    Object to move data from flat JSON files into MongoDB.
    Zipped data can be downloaded at
    https://www.propublica.org/datastore/dataset/congressional-data-bulk-legislation-bills

    TODO:
        1. Export directly to postgres
    """

    def __init__(self, congress):
        self.dir = tempfile.TemporaryDirectory()
        self.congress = str(congress)
        print(f"{self.dir.name}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.dir.cleanup()

    def extract_file(self, f):
        with zipfile.ZipFile(f, "r") as zip_ref:
            zip_ref.extractall(self.dir.name)

    def get_filetypes(self, ftype=".json"):
        found = []
        for root, dirs, files in os.walk(self.dir.name):
            for file in files:
                if file.endswith(ftype):
                    found.append(os.path.join(root, file))
        return found

    def migrate(self, files, loc):
        p = pathlib.Path(loc)
        p.mkdir(parents=True, exist_ok=True)
        for f in files:
            simpleid = f.split("/")[-2]
            shutil.move(f, os.path.join(loc, self.congress + "." + simpleid + ".json"))

    def migrate_to_psql(self, files, db='congress'):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASSWORD'),
            database=db,
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.execute(
            TABLE_CREATE_BILLS
        )
        conn.commit()
        conn.close()
        nerrors = 0
        try:
            for file in files:
                data = json.load(open(file, "r"))
                vals = self.prep_for_sql(data)
                res = self.to_psql(vals, db=db)
                if not res:
                    nerrors += 1
        except:
            nerrors += 1

    @staticmethod
    def to_psql(v, db='congress'):
        conn = None
        try:
            conn = psycopg2.connect(
                user=os.environ.get('PSQL_USER'),
                password=os.environ.get('PSQL_PASSWORD'),
                database=db,
                host=os.environ.get('PSQL_HOST'),
                port=os.environ.get('PSQL_PORT', '5432')
            )
            cursor = conn.cursor()
            command = cursor.mogrify("""
            INSERT INTO bills 
                (actions, amendments, bill_id, bill_type, committees, congress, cosponsors, enacted_as,
                history, introduced_at, number, official_title, popular_title, related_bills,
                short_title, sponsor, status, status_at, subjects, subjects_top_term, summary,
                titles, updated_at, bill_body)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT ON CONSTRAINT bills_pkey DO NOTHING
            """, (v))
            cursor.execute(command)
            conn.commit()
            conn.close()
        except Exception as e:
            if conn:
                conn.close()
                print(e)
                return False
            raise e

        return True

    @staticmethod
    def prep_for_sql(d):
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
                return False
            else:
                if isinstance(d[v], list) or isinstance(d[v], dict):
                    vals.append(json.dumps(d[v]))
                else:
                    vals.append(d[v])

        return vals

    def add_bill_bodies(self, db='congress', table='bills'):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASSWORD'),
            database=db,
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT', '5432')
        )
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT 
                    bill_type, bill_id, congress 
                FROM 
                    {table} 
                WHERE 
                    (bill_body = 'NO BODY' OR bill_body IS NULL) 
                    AND congress > 100
            """
        )
        df = pd.DataFrame(
            cursor.fetchall(), columns=['bill_type', 'bill_id', 'congress']
        )
        conn.close()

        for _, row in df.to_dict(orient='records'):
            body = self.fetch_bill_text(row['bill_type'], row['bill_id'], row['congress'])
            if body is not None:
                conn = psycopg2.connect(
                    user=os.environ.get('PSQL_USER'),
                    password=os.environ.get('PSQL_PASSWORD'),
                    database=db,
                    host=os.environ.get('PSQL_HOST'),
                    port=os.environ.get('PSQL_PORT', '5432')
                )
                cursor = conn.cursor()
                command = f"""
                    UPDATE 
                        bills
                    SET 
                        bill_body = '{body}' 
                    WHERE 
                        bill_type = '{row['bill_type']}' 
                        AND bill_id = '{row['bill_id']}' 
                        AND congress = {row['congress']}
                """
                cursor.execute(command)
                conn.commit()
                conn.close()

    @staticmethod
    def fetch_bill_text(bill_type, bill_id, congress):
        # found ordinal func on stack overflow - not perfect but works for this
        # ordinal = lambda n: "%d%s" % (n, "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])
        mapper = {
            "hconres": "house-concurrent-resolution",
            "hjres": "house-joint-resolution",
            "hr": "house-bill",
            "hres": "house-resolution",
            "s": "senate-bill",
            "sconres": "senate-concurrent-resolution",
            "sjres": "senate-joint-resolution",
            "sres": "senate-resolution"
        }
        if bill_type not in mapper:
            return None

        bill_num = bill_id.split("-")[0]
        bill_num = "".join([i for i in bill_num if i.isdigit()])

        mapped_btype = mapper[bill_type]
        ord_cong = BillsToDB.ordinal(int(congress))
        url = f"https://www.congress.gov/bill/{ord_cong}-congress/{mapped_btype}/{bill_num}/text?format=txt"

        try:
            p = BeautifulSoup(requests.get(url).text)
            body = p.find(id="billTextContainer").get_text()
        except AttributeError:
            print(bill_type, bill_id, congress, url)
            return None
        return body

    @staticmethod
    def ordinal(n):
        return "%d%s" % (n, "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])

    # started using mongo but quickly remembered why i hate mongo
    # def migrate_to_mongo(self, files, db="congress", collection="bills", batchsize=1000):
    #     client = MongoClient(
    #         os.environ.get('MONGO_HOST', 'localhost'),
    #         os.environ.get('MONGO_PORT', 27017)
    #     )
    #     coll = client[db][collection]
    #
    #     batches = np.array_split(files, int(len(files) / batchsize) + 1)
    #
    #     for batch in batches:
    #         batch_data = []
    #         for file in batch:
    #             data = json.load(open(file, "r"))
    #             batch_data.append(data)
    #         coll.insert_many(batch_data)
    #
    #     client.close()
    #
    # def migrate_to_mongo_one_at_time(self, files, db="congress", collection="bills"):
    #     client = MongoClient(
    #         os.environ.get('MONGO_HOST', 'localhost'),
    #         os.environ.get('MONGO_PORT', 27017)
    #     )
    #     db = client[db]
    #     coll = db[collection]
    #
    #     nerrors = 0
    #     try:
    #         for file in files:
    #             data = json.load(open(file, "r"))
    #             res = coll.insert_one(data)
    #             if not res:
    #                 nerrors += 1
    #     except:
    #         nerrors += 1
    #
    #     print(len(files), nerrors)
    #
    #     client.close()






