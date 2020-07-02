from congress.etl.members import LegislatorsToDB
import os
from argparse import ArgumentParser


def main(db, table, chamber, congress, get_all):
    assert db is not None
    legis = LegislatorsToDB(os.environ['PRO_PUBLICA_API_KEY'], db, table)
    if get_all:
        legis.fetch_members(
            {'house': [str(i) for i in range(102, 116)], 
             'senate': [str(i) for i in range(80, 116)]}
        )
    else:
        assert chamber in ('house', 'senate')
        if chamber == 'house':
            assert int(congress) in list(range(102, 116))
        else:
            assert int(congress) in list(range(80, 116))
        legis.fetch_members({chamber: [str(congress)]})

if __name__ == '__main__':
    parser = ArgumentParser(
        description='Scrape congress member data from propublica'
    )
    parser.add_argument('--db', help='Name of DB', type=str, default='congress')
    parser.add_argument('--table', help='Name of table', type=str, default='members')
    parser.add_argument('--chamber', help='\'house\' or \'senate\'')
    parser.add_argument('--congress', help='Congress number (102-115 for house, 80-115 for senate)')
    parser.add_argument('--all', help='scrape everything', default=False, action='store_true')
    args = parser.parse_args()

    main(args.db, args.table, args.chamber, args.congress, args.all)
