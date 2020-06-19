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

    def __init__(self, api_key):
        self.api_key = api_key

    def fetch_members(self, params, db='congress'):
        conn = psycopg2.connect(
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASSWORD'),
            database=db,
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

        LegislatorsToDB.to_psql(df, db=db)

    @staticmethod
    def to_psql(df, db='congress'):
        for row in df.to_dict(orient='records'):
            q = """INSERT INTO members 
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
                password=os.environ.get('PSQL_PASSWORD'),
                database=db,
                host=os.environ.get('PSQL_HOST'),
                port=os.environ.get('PSQL_PORT')
            )
            cursor = conn.cursor()
            command = cursor.mogrify(q, (LegislatorsToDB.prep_member_for_psql(row)))
            cursor.execute(command)
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
            except KeyError:
                vals.append('')
        return vals

    def _base_call(self, endpoint, headers=None, params=None):
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

    # def fetch_bills_by_member(self, params, full_text=False, tabularize=True):
    #     """Get all bills for a given member of congress.
    #
    #     params: dict of member_id key with bill type as values
    #             legal values:
    #                 member_id: see id field from members response
    #                 typ: introduced, updated, active, passed, enacted, vetoed
    #
    #     returns list of json responses(1 for each id-type pair) or dataframe
    #     """
    #     responses = []
    #     for mid, typ in params.items():
    #         for t in typ:
    #             endpoint = f"https://api.propublica.org/congress/v1/members/{mid}/bills/{t}.json"
    #             offset = 0
    #             r = self._base_call(endpoint, params={"offset": str(offset)})
    #             results = r["results"]
    #             responses.append(r)
    #             offset += 20
    #             while offset < results[0]["num_results"]:
    #                 offset += 20
    #                 responses.append(self._base_call(endpoint, params={"offset": str(offset)}))
    #
    #     if tabularize:
    #         all_bills = []
    #         for r in responses:
    #             bills = []
    #             for res in r["results"]:
    #                 bills += res["bills"]
    #             bills = pd.DataFrame(bills)
    #             bills["id"] = res["id"]
    #             bills["name"] = res["name"]
    #             all_bills.append(bills)
    #         return pd.concat(all_bills).reset_index(drop=True)
    #     else:
    #         return responses

    # @staticmethod
    # def fetch_bill_text(endpoint):
    #     p = BeautifulSoup(requests.get(endpoint + "/text?format=txt").text)
    #     body = p.find(id="billTextContainer").get_text()
    #     return body
