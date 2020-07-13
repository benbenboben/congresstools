import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import numpy as np
import fractions
import re
import psycopg2
import traceback

from congress.etl.vars import TABLE_CREATE_HOUSE_VOTES, TABLE_CREATE_HOUSE_VOTES_INDIVIDUAL


class HouseVotes2DB(object):

    def __init__(self, db, table):
        self.database = db
        self.table = table

    def votes_to_psql(self, year):
        rolls = self.roll_call_summary_to_psql(year)
        dfs = []
        for r in rolls:
            this_df = self.roll_call_single_to_psql(year, r)
            dfs.append(this_df)
            break

        for x in range(0, len(dfs), 20):
            print(f'{x} to {x + 20} out of {len(dfs)}', end='\r')
            df = pd.concat(dfs[x: x + 20])
            self.individual_to_psql(df)

    def roll_call_summary_to_psql(self, year):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database=self.database,
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.execute(
            TABLE_CREATE_HOUSE_VOTES
        )
        conn.commit()
        conn.close()

        table = self.roll_call_summary(year)

        self.summary_to_psql(table)

        return list(table['rollcall'])

    def roll_call_summary(self, year):
        url = f"http://clerk.house.gov/evs/{year}"
        rolls = ['ROLL_000.asp'] + [f'ROLL_{i * 100}.asp' for i in range(1, 20)]

        tables = []
        for r in rolls:
            try:
                data = requests.get(os.path.join(url, r))
                soup = BeautifulSoup(data.text)
                table = self._roll_call_table(soup)
                table['date'] = str(year) + '-' + table['date']
                table['date'] = pd.to_datetime(table['date'], format='%Y-%d-%b', errors='coerce')
                tables.append(table)
            except:
                pass

        df = pd.concat(tables, ignore_index=True)
        return df

    @staticmethod
    def prep_summary_for_psql(d):
        vals = []
        for c in ['rollcall', 'date', 'bill', 'session',
                  'congress', 'type', 'result', 'title']:
            try:
                vals.append(d[c])
            except KeyError:
                vals.append('')
        return vals

    def summary_to_psql(self, df):
        for row in df.to_dict(orient='records'):
            q = f"""INSERT INTO {self.table} 
                (rollcall, date, bill, session, congress, type, result, title)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT ON CONSTRAINT house_votes_pkey DO NOTHING
            """
            conn = psycopg2.connect(
                user=os.environ.get('PSQL_USER'),
                password=os.environ.get('PSQL_PASS'),
                database=self.database,
                host=os.environ.get('PSQL_HOST'),
                port=os.environ.get('PSQL_PORT')
            )
            cursor = conn.cursor()
            command = cursor.mogrify(q, (HouseVotes2DB.prep_summary_for_psql(row)))
            cursor.execute(command)
            conn.commit()
            conn.close()

    def _roll_call_table(self, soup):
        table = soup.find("table")
        rows = table.find_all('tr')
        session_num = re.search('(Congress - )([1-2])', soup.text).group(2)
        dfdata = []
        for row in rows:
            cells = row.find_all('td')
            try:
                link = cells[0].a['href']
                roll = cells[0].text
                date = cells[1].text
                bill = cells[2].text.replace(" ", "").lower()
                congress = "".join([i for i in cells[2].a['href'].split("/")[4] if i.isdigit()])
                typ = cells[3].text
                res = cells[4].text
                try:
                    title = cells[5].text
                except Exception as e:
                    print(e)

                    title = 'NONE'

                dfdata.append(
                    {'link': link,
                     'rollcall': roll,
                     'date': date,
                     'bill': bill,
                     'session': session_num,
                     'congress': congress,
                     'type': typ,
                     'result': res,
                     'title': title})
            except Exception as e:
                print(e)
        return pd.DataFrame(dfdata)

    def roll_call_single(self, year, roll):
        roll = str(roll)
        nz = 3 - len(roll)
        roll = ('0' * nz) + roll
        url = f'http://clerk.house.gov/evs/{year}/roll{roll}.xml'
        print(url)
        data = requests.get(url)
        soup = BeautifulSoup(data.text)

        congress = soup.find('congress').text
        session = ''.join([i for i in soup.find('session').text if i.isdigit()])
        # chamber = soup.find('chamber').text
        rollcall = soup.find('rollcall-num').text
        vq = soup.find('vote-question').text
        vt = soup.find('vote-type').text
        # vr = soup.find('vote-result').text
        # date = soup.find('action-date').text
        # tofd = soup.find('action-time').text
        title = soup.find('vote-desc').text

        votes = []
        for i in soup.find_all('recorded-vote'):
            votes.append(
                {**i.find('legislator').attrs, 
                 **{'vote': i.find('vote').text},
                 **{'last_name': i.find('legislator').text.split(' ')[0]}}
            )
        df = pd.DataFrame(votes)
        df.columns = [i.replace('-', '_') for i in df.columns]
        df['congress'] = congress
        df['session'] = session
        df['rollcall'] = rollcall
        df['vote_question'] = vq
        df['vote_type'] = vt
        # df['vote_result'] = vr
        df['title'] = title
        
        return df

    def roll_call_single_to_psql(self, year, roll):

        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database=self.database,
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.execute(
            TABLE_CREATE_HOUSE_VOTES_INDIVIDUAL
        )
        conn.commit()
        conn.close()

        df = self.roll_call_single(year, roll)
        return df
        # self.individual_to_psql(df)

    def individual_to_psql(self, df):
        q = f"""INSERT INTO {self.table + '_individual'} 
                (name_id, last_name, party, state, vote,
                 congress, session, rollcall, vote_question, title)
                VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT ON CONSTRAINT house_votes_individual_pkey DO NOTHING

            """
        vals = []
        for row in df.to_dict(orient='records'):
            # command = cursor.mogrify(q, (HouseVotes2DB.prep_individual_for_psql(row)))
            # cursor.execute(command)
            vals.append(HouseVotes2DB.prep_individual_for_psql(row))
           
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database=self.database,
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.executemany(q, vals)
        conn.commit()
        conn.close()


    @staticmethod
    def prep_individual_for_psql(d):
        vals = []
        for c in ['name_id', 'last_name', 'party', 'state', 'vote',
                  'congress', 'session', 'rollcall', 'vote_question', 'title']:
            try:
                vals.append(d[c])
            except KeyError:
                # print(c)
                vals.append('')
        return vals


