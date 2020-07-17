import os
import requests
import json
import psycopg2
import pandas as pd
from congress.etl.vars import TABLE_CREATE_MEMBERS
from congress.etl.base import APIError, DataToDB


class LegislatorsToDB(DataToDB):
    """
    Move data about legislators into PostegreSQL
    """
    def fetch(self, params):
        """Get members of congress from pro publica.

        :param params: grid of chamber and congress number
        :type params: dict with keys being chamber ('house', or 'senate') and 
                      values being a list of congress numbers
        """
        jsons = []
        for chamber, congress_num in params.items():
            for c in congress_num:
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

        return df

    def create_table(self):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASS'),
            database='congress',
            host=os.environ.get('PSQL_HOST'),
            port=os.environ.get('PSQL_PORT')
        )
        cursor = conn.cursor()
        cursor.execute(TABLE_CREATE_MEMBERS)
        conn.commit()
        conn.close()

    def cleanse(self, d):
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
            except KeyError:
                vals.append(0)
           
        return vals

    def to_psql(self, vals):
        """Transform DataFrame into rows for PostgreSQL.

        :param df: tabularized data from API
        :type df: pd.DataFrame
        """
        query = """
        INSERT INTO members
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
                database='congress',
                host=os.environ.get('PSQL_HOST'),
                port=os.environ.get('PSQL_PORT')
            )
        cursor = conn.cursor()
        cursor.executemany(query, vals)
        conn.commit()
        conn.close()
