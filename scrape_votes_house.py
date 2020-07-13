from congress.etl.house_votes import HouseVotes2DB
import requests
import os
from argparse import ArgumentParser


def main(db, table, year):
    assert int(year) >= 1991
    vote_scraper = HouseVotes2DB(db, table)
    vote_scraper.votes_to_psql(year)

if __name__ == '__main__':
    parser = ArgumentParser(
        description='Scrape votes from '
    )
    parser.add_argument('--db', help='Name of DB', type=str)
    parser.add_argument('--table', help='Table name', type=str, default='house_votes')
    parser.add_argument('--year', help='Congress Number', type=int)
    args = parser.parse_args()

    main(args.db, args.table, args.year)
