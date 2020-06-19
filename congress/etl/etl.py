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








def test_congress_billbody_avail(bill_id, bill_type, congress):
    body = fetch_bill_text(bill_type, bill_id, congress)
    if body is not None:
        return True
    else:
        return False
            
            






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
        vals.append(d[c])
    return vals


def df_to_psql(df, db='congress'):
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
        command = cursor.mogrify(q, (prep_member_for_psql(row)))
        cursor.execute(command)
        conn.commit()
        conn.close()

    
    
    
# def fetch_bodies(congress):
#     client = MongoClient(
#         os.environ.get('MONGO_HOST', 'localhost'), 
#         os.environ.get('MONGO_PORT', 27017)
#     )
#     db = client["congress"]
#     coll = db["bills"]
#     data = coll.find(
#         {"congress": {"$eq": congress}, "bill_id": {"$exists": True}, "bill_body": {"$eq": "NO BODY"}},
#         {"bill_type": True, "bill_id": True, "congress": True}
#     )

#     errors = []        
#     for _, entry in enumerate(data):
#         try:
#             body = fetch_bill_text(entry["bill_type"], entry["bill_id"], entry["congress"])
#             if body is None:
#                 adder = {"bill_body": "NO BODY"}
#             else:
#                 adder = {"bill_body": body}
#             coll.update_one({"_id": entry["_id"]}, {"$set": adder})
#         except Exception as e:
#             errors.append(e)

#     client.close()



# def push_to_psql(congress):
#     client = MongoClient(
#         os.environ.get('MONGO_HOST', 'localhost'), 
#         os.environ.get('MONGO_PORT', 27017)
#     )
#     db = client["congress"]
#     coll = db["bills"]
#     data = coll.find({'congress': {'$eq': str(congress)}})
#     client.close()
    
#     # these should be bulk inserted but i already wrote the annoying to_psql fxn...
#     for d in data:
#         v = prep_for_sql(d)
#         to_psql(v)
