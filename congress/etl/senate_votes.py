import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import fractions
import re
import psycopg2

from congress.etl.vars import TABLE_CREATE_SENATE_VOTES, TABLE_CREATE_SENATE_VOTES_INDIVIDUAL


class SenateVotes2DB(object):

    def __init__(self):
        pass

    def get_congress_year_summary(self, soup):
        header = soup.find(id='legislative_header').find('h1').text
        header = header.lower().split(' ')
        congress_num = ''.join([i for i in header[3] if i.isdigit()])
        year = ''.join([i for i in header[-1] if i.isdigit()])
        return congress_num, year

    def roll_call_summary_to_psql(self, congress, db='congress'):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASSWORD'),
            database=db,
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.execute(
            TABLE_CREATE_SENATE_VOTES
        )
        conn.commit()
        conn.close()

        table = self.roll_call_summary(congress)

        SenateVotes2DB.summary_to_psql(table, db=db)

    @staticmethod
    def summary_to_psql(df, db='congress'):
        for row in df.to_dict(orient='records'):
            q = """INSERT INTO members 
                (congress, rollcall, result, title, bill, date, session)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT ON CONSTRAINT house_votes_pkey DO NOTHING
            """
            conn = psycopg2.connect(
                user=os.environ.get('PSQL_USER'),
                password=os.environ.get('PSQL_PASSWORD'),
                database=db,
                host=os.environ.get('PSQL_HOST'),
                port=os.environ.get('PSQL_PORT')
            )
            cursor = conn.cursor()
            command = cursor.mogrify(q, (SenateVotes2DB.prep_summary_for_psql(row)))
            cursor.execute(command)
            conn.commit()
            conn.close()

    @staticmethod
    def prep_summary_for_psql(d):
        vals = []
        for c in ['congress', 'rollcall', 'result', 'title', 'bill', 'date', 'session']:
            try:
                vals.append(d[c])
            except KeyError:
                vals.append('')
        return vals

    def roll_call_summary(self, congress):
        # 'https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_101_1.htm'
        dfdata = []
        for session in [1, 2]:
            data = requests.get(
                f'https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{congress}_{session}.htm'
            )
            soup = BeautifulSoup(data.text)
            congress_num, congress_year = self.get_congress_year_summary(soup)

            table = soup.find('table')
            rows = table.find_all('tr')

            for row in rows:
                try:
                    row = row.replace(r'\xa0', ' ')
                except TypeError:
                    pass
                cells = row.find_all('td')
                if not cells:
                    continue

                vote_id = cells[0].text.split(' ')[1]
                try:
                    vote_id = vote_id[:vote_id.index(r'(')].strip()
                except:
                    pass
                result = cells[1].text
                title = cells[2].text
                bill = ''.join([i for i in cells[3].text if i.isalnum()]).lower()
                mo, day = cells[4].text[:3], cells[4].text[-2:]
                date = '-'.join([mo, day])
                dfdata.append({
                    'congress': congress_num,
                    # 'link': link,
                    'rollcall': vote_id,
                    'result': result,
                    'title': title,
                    'bill': bill,
                    'date': str(congress_year) + '-' + date,
                    'session': session
                })

        return pd.DataFrame(dfdata)

    def roll_call_single(self, congress, session, rollcall):
        rollcall = str(rollcall)
        pads = 5 - len(rollcall)
        rollcall = (pads * '0') + str(rollcall)
        url = f"https://www.senate.gov/legislative/LIS/roll_call_lists/roll_call_vote_cfm.cfm?congress={congress}&session={session}&vote={rollcall}"
        # data = requests.get('https://www.senate.gov/legislative/LIS/roll_call_lists/roll_call_vote_cfm.cfm?congress=102&session=1&vote=00280')
        data = requests.get(url)
        soup = BeautifulSoup(data.text)

        header = soup.find('section', {'id': 'legislative_header'})
        header = header.text.strip().split(' ')
        header = [i for i in header if i]
        congress_num = ''.join([i for i in header[3] if i.isdigit()])
        session_num = ''.join([i for i in header[6] if i.isdigit()])
        z = soup.find_all('div', {'class': 'contenttext'})
        meta = z[0].text.strip()
        question = meta[0: meta.index('Vote Number: ')].strip().replace('\n', '')[9:]
        vote_num = meta[
                   meta.index('Vote Number: '): meta.index('Vote Date: ')
                   ].strip().replace('\n', '').replace('\t', '')[len('Vote Number: '):].strip()
        required_majority = int(round(fractions.Fraction(
            meta[meta.index('Required For Majority: '): meta.index('Vote Result: ')][
            len('Required For Majority: '):].strip()
        ) * 100, 0))

        votes = soup.find('span', {'class': 'contenttext'}).text.split('\n')
        votelist = []
        for v in votes:
            if not v:
                continue
            try:
                paren_index = v.index(r'(') - 1
            except:
                print(v)
            name = v[:paren_index]
            if ',' in name:
                surname = name.split(',')[0].strip()
                firstname = name.split(',')[-1].strip()
            else:
                surname = name.strip()
                firstname = ''

            v = v[paren_index + 1:]
            v = v.split(', ')
            vote = v[-1]
            party = v[0].split('-')[0][1:]
            state = v[0].split('-')[1][:-1]

            votelist.append(
                {'firstname': firstname,
                 'surname': surname,
                 'state': state,
                 'party': party,
                 'vote': vote,
                 'congress': congress_num,
                 'session': session_num,
                 'question': question,
                 'required_majority': required_majority,
                 'rollcall': vote_num
                 }
            )
        return pd.DataFrame(votelist)

    def roll_call_single_to_psql(self, congress, session, roll_call, db='congress'):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASSWORD'),
            database=db,
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.execute(
            TABLE_CREATE_SENATE_VOTES_INDIVIDUAL
        )
        conn.commit()
        conn.close()

        table = self.roll_call_single(congress, session, roll_call)

        SenateVotes2DB.individual_to_psql(table, db=db)

    @staticmethod
    def individual_to_psql(df, db='congress'):
        for row in df.to_dict(orient='records'):
            q = """INSERT INTO members 
                (firstname, surname, state, party, vote, congress, session, question, required_majority, rollcall)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT ON CONSTRAINT house_votes_pkey DO NOTHING
            """
            conn = psycopg2.connect(
                user=os.environ.get('PSQL_USER'),
                password=os.environ.get('PSQL_PASSWORD'),
                database=db,
                host=os.environ.get('PSQL_HOST'),
                port=os.environ.get('PSQL_PORT')
            )
            cursor = conn.cursor()
            command = cursor.mogrify(q, (SenateVotes2DB.prep_individual_for_psql(row)))
            cursor.execute(command)
            conn.commit()
            conn.close()

    @staticmethod
    def prep_individual_for_psql(d):
        vals = []
        for c in ['firstname', 'surname', 'state', 'party', 'vote', 'congress', 'session',
                  'question', 'required_majority', 'rollcall']:
            try:
                vals.append(d[c])
            except KeyError:
                vals.append('')
        return vals
