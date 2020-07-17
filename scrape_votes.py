from congress.etl.votes import VotesToDB
import requests
import os
from argparse import ArgumentParser


def main(congress, chamber):
    assert congress in list(range(102, 116))
    votes_to_db = VotesToDB()
    votes_to_db.run_pipeline(congress, chamber)        
        

if __name__ == '__main__':
    parser = ArgumentParser(
        description='Push data from ProPublica on bills to Postgres'
    )
    parser.add_argument(
        '--congress', type=int, help='Congress number (93-115).  Bills for both chambers will be fetched.'
    )
    parser.add_argument(
        '--chamber', type=str, help='Either senate or house.', default=None
    )

    args = parser.parse_args()

    main(args.congress, args.chamber)
