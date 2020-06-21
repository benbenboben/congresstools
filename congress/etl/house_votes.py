import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import fractions
import re
import psycopg2
import traceback

from congress.etl.vars import TABLE_CREATE_HOUSE_VOTES, TABLE_CREATE_HOUSE_VOTES_INDIVIDUAL


class HouseVotes2DB(object):

    def __init__(self):
        pass

    def roll_call_summary_to_psql(self, year, db='congress'):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database=db,
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

        HouseVotes2DB.summary_to_psql(table)

    def roll_call_summary(self, year, to_postgres=True, db='congress'):
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
        # df['date'] = str(year) + '-' + df['date'].astype(str)
        # df['date'] = pd.to_datetime(df['date'], format='%Y-%d-%b', errors='coerce')
        #
        # return df

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

    @staticmethod
    def summary_to_psql(df, db='congress'):
        for row in df.to_dict(orient='records'):
            q = """INSERT INTO house_votes 
                (rollcall, date, bill, session, congress, type, result, title)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT ON CONSTRAINT house_votes_pkey DO NOTHING
            """
            conn = psycopg2.connect(
                user=os.environ.get('PSQL_USER'),
                password=os.environ.get('PSQL_PASS'),
                database=db,
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

    def roll_call_single(self, year, roll, to_posgres=True, db='congress'):
        # 'http://clerk.house.gov/cgi-bin/vote.asp?year=2019&rollnumber=99'
        # url = f'http://clerk.house.gov/cgi-bin/vote.asp?year={year}&rollnumber={roll}'
        roll = str(roll)
        nz = 3 - len(roll)
        roll = ('0' * nz) + roll
        url = f'http://clerk.house.gov/evs/{year}/roll{roll}.xml'
        print(url)
        data = requests.get(url)
        soup = BeautifulSoup(data.text)

        congress = soup.find('congress').text
        session = ''.join([i for i in soup.find('session').text if i.isdigit()])
        chamber = soup.find('chamber').text
        rollcall = soup.find('rollcall-num').text
        vq = soup.find('vote-question').text
        vt = soup.find('vote-type').text
        vr = soup.find('vote-result').text
        # date = soup.find('action-date').text
        # tofd = soup.find('action-time').text
        title = soup.find('vote-desc').text

        votes = []
        for i in soup.find_all('recorded-vote'):
            votes.append(
                {**i.find('legislator').attrs, **{'vote': i.find('vote').text}}
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

    def roll_call_single_to_psql(self, year, roll, db='congress'):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database=db,
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
        HouseVotes2DB.individual_to_psql(df, db=db)

    @staticmethod
    def individual_to_psql(df, db='congress'):
        for row in df.to_dict(orient='records'):
            q = """INSERT INTO house_votes_individual 
                (name_id, unaccented_name, party, state, vote,
                 congress, session, rollcall, vote_question, title)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            conn = psycopg2.connect(
                user=os.environ.get('PSQL_USER'),
                password=os.environ.get('PSQL_PASS'),
                database=db,
                host=os.environ.get('PSQL_HOST'),
                port=os.environ.get('PSQL_PORT')
            )
            cursor = conn.cursor()
            command = cursor.mogrify(q, (HouseVotes2DB.prep_individual_for_psql(row)))
            cursor.execute(command)
            conn.commit()
            conn.close()

    @staticmethod
    def prep_individual_for_psql(d):
        vals = []
        for c in ['name_id', 'unaccented_name', 'party', 'state', 'vote',
                  'congress', 'session', 'rollcall', 'vote_question', 'title']:
            try:
                vals.append(d[c])
            except KeyError:
                print(f'{c} is all fucked up')
                print(d)
                vals.append('')
        return vals


