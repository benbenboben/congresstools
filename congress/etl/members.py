import os
import requests
import json
import psycopg2
import pandas as pd
from congress.etl.vars import TABLE_CREATE_MEMBERS


class APIError(Exception):

    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = "API Error raised"

    def __str__(self):
        print(self.message)


class LegislatorsToDB(object):
    """
    Move data about legislators into PostegreSQL
    """

    def __init__(self, api_key, db, table):
        """Initialize object

        :param api_key: api key from pro publica
        :type api_key: str
        """
        self.api_key = api_key
        self.database = db
        self.table = table

    def fetch_members(self, params):
        """Get members of congress from pro publica.

        :param params: grid of chamber and congress number
        :type params: dict with keys being chamber ('house', or 'senate') and 
                      values being a list of congress numbers
        """
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database=self.database,
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.execute(
            TABLE_CREATE_MEMBERS
        )
        conn.commit()
        conn.close()

        jsons = []
        for chamber, congress_num in params.items():
            for c in congress_num:
                print(chamber, c)
                endpoint = f"https://api.propublica.org/congress/v1/{c}/{chamber}/members.json"
                data = self._base_call(endpoint)
                jsons.append(data)

        all_members = []
        for blob in jsons:
            results = blob["results"]
            for r in results:
                members = pd.DataFrame(r["members"])
                members["congress"] = r["congress"]
                members["chamber"] = r["chamber"]
                all_members.append(members)
        df = pd.concat(all_members)

        self.to_psql(df)

    def to_psql(self, df):
        """Transform DataFrame into rows for PostgreSQL.

        :param df: tabularized data from API
        :type df: pd.DataFrame
        """
        query = f"""
        INSERT INTO {self.table} 
            (api_uri, at_large, chamber, congress, contact_form, cook_pvi, crp_id,
             cspan_id, date_of_birth, district, dw_nominate, facebook_account,
             fax, fec_candidate_id, first_name, gender, geoid, google_entity_id,
             govtrack_id, icpsr_id, id, ideal_point, in_office, last_name, last_updated, 
             leadership_role, lis_id, middle_name, missed_votes, missed_votes_pct,
             next_election, ocd_id, office, party, phone, rss_url, senate_class,
             seniority, short_title, state, state_rank, suffix, title, total_present,
             total_votes, twitter_account, url, votes_against_party_pct, votes_with_party_pct,
             votesmart_id, youtube_account)
        VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
             %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT ON CONSTRAINT members_pkey DO NOTHING
        """
        conn = psycopg2.connect(
                user=os.environ.get('PSQL_USER'),
                password=os.environ.get('PSQL_PASS'),
                database=self.database,
                host=os.environ.get('PSQL_HOST'),
                port=os.environ.get('PSQL_PORT')
            )
        cursor = conn.cursor()
        vals = []
        for row in df.to_dict(orient='records'):
            vals.append(LegislatorsToDB.prep_member_for_psql(row))
        cursor.executemany(query, vals)
        conn.commit()
        conn.close()

    @staticmethod
    def prep_member_for_psql(d):
        vals = []
        for c in ['api_uri', 'at_large', 'chamber', 'congress', 'contact_form', 'cook_pvi',
                  'crp_id', 'cspan_id', 'date_of_birth', 'district', 'dw_nominate', 'facebook_account',
                  'fax', 'fec_candidate_id', 'first_name', 'gender', 'geoid', 'google_entity_id',
                  'govtrack_id', 'icpsr_id', 'id', 'ideal_point', 'in_office', 'last_name', 'last_updated',
                  'leadership_role', 'lis_id', 'middle_name', 'missed_votes', 'missed_votes_pct',
                  'next_election', 'ocd_id', 'office', 'party', 'phone', 'rss_url', 'senate_class',
                  'seniority', 'short_title', 'state', 'state_rank', 'suffix', 'title', 'total_present',
                  'total_votes', 'twitter_account', 'url', 'votes_against_party_pct', 'votes_with_party_pct',
                  'votesmart_id', 'youtube_account']:
            if c == 'date_of_birth' and d[c] == '':
                d[c] = '1900-01-01'
            try:
                vals.append(d[c])
                if d[c] == 'David':
                    print(d)
            except KeyError:
                vals.append(0)
           
        return vals

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
            headers = {"X-API-Key": self.api_key}
        else:
            headers = {**headers, **{"X-API-Key": self.api_key}}

        params = {} if params is None else params

        r = requests.get(endpoint, headers=headers, params=params)
        if r.status_code != 200:
            raise APIError(f"Error fetching member data -- received {r.status_code}")
        else:
            return json.loads(r.text)
