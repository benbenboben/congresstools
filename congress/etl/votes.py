import requests
import os
import pandas as pd
import numpy as np
import json
import re
import psycopg2
import traceback
import time

from congress.etl.base import DataToDB, APIError
from congress.etl.vars import TABLE_CREATE_VOTES


class VotesToDB(DataToDB):

    def fetch(self, congress, the_chamber=None):
        if the_chamber is None:
            the_chamber = ['house', 'senate']
        else:
            the_chamber = [the_chamber]
        totals = 0
        all_data = []
        url_root = 'https://api.propublica.org/congress/v1/'
        rcall = 1
        errors_in_a_row = 0
        for session in [1, 2]:
            for chamber in the_chamber:
                while True:
                    url = url_root + f'{congress}/{chamber}/sessions/{session}/votes/{rcall}.json'
                    try:
                        data = self._base_call(url)
                    except APIError:
                        print(f'ERROR: {url}')
                        break
                    if data['status'].lower() != 'ok':
                        print(data)
                        print(f'ERROR: {url}')
                        errors_in_a_row += 1
                        if errors_in_a_row >= 3:
                            rcall = 1
                            errors_in_a_row = 0
                            break
                        else:
                            rcall += 1
                            totals += 1
                            continue
                    all_data.append(data['results']['votes']['vote'])
                    rcall += 1
                    totals += 1
                    errors_in_a_row = 0
                    # time.sleep(0.5)
                    print(congress, chamber, rcall, totals, end='\r')

        print(f'Gathered {totals} roll call votes')
        return pd.DataFrame(all_data)

    def cleanse(self, row):
        this_row = []
        for c in ['congress', 'session', 'chamber', 'roll_call', 'source', 'url', 'bill', 'amendment', 
                  'question', 'question_text', 'description', 'vote_type', 'date', 'time', 'result', 
                  'tie_breaker', 'tie_breaker_vote', 'document_number', 'document_title', 'democratic', 
                  'republican', 'independent', 'total', 'positions']:
            try:
                if type(row[c]) in (dict, list):
                    this_row.append(json.dumps(row[c]))
                else:
                    this_row.append(row[c])
            except KeyError:
                this_row.append('')
        return this_row

    def create_table(self):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database='congress',
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.execute(TABLE_CREATE_VOTES)
        conn.commit()
        conn.close()

    def to_psql(self, vals):
        query = """
        INSERT INTO votes (
            congress, session, chamber, roll_call, source, url, bill, amendment, 
            question, question_text, description, vote_type, date, time, result, 
            tie_breaker, tie_breaker_vote, document_number, document_title, democratic, 
            republican, independent, total, positions
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )         
        ON CONFLICT ON CONSTRAINT votes_pkey DO NOTHING
        """
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database='congress',
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT', '5432')
        )
        cursor = conn.cursor()
        cursor.executemany(query, vals)
        conn.commit()
        conn.close()
    
                

    
                