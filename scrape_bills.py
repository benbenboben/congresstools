from congress.etl.bills import BillsToDB
import requests
import os
from argparse import ArgumentParser


def main(congress, bodies):
    assert congress in list(range(93, 116))

    if not bodies:
        with BillsToDB() as bills_to_db:
            bills_to_db.run_pipeline(congress)
    else:
        raise NotImplementedError('Adding bill bodies is untested')
            

if __name__ == '__main__':
    parser = ArgumentParser(
        description='Push data from ProPublica on bills to Postgres'
    )
    parser.add_argument(
        '--congress', type=int, help='Congress number (93-115).  Bills for both chambers will be fetched.')
    parser.add_argument('--bodies', help='Scrape congressional bodies', 
                        action='store_true', default=False)
    args = parser.parse_args()

    main(args.congress, args.bodies)
