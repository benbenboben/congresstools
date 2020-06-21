from congress.etl.members import LegislatorsToDB
import os
from argparse import ArgumentParser


def main(db):
    assert db is not None
    legis = LegislatorsToDB(os.environ['PRO_PUBLICA_API_KEY'])
    _ = legis.fetch_members(
        {"house": [str(i) for i in range(102, 116)], 
         "senate": [str(i) for i in range(80, 116)]}, db=db)


if __name__ == '__main__':
    parser = ArgumentParser(
        description='Scrape congress member data from propublica'
    )
    parser.add_argument('--db', help='Name of DB', type=str)
    args = parser.parse_args()

    main(args.db)
