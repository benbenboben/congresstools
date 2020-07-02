from congress.etl.bills import BillsToDB
import requests
import os
from argparse import ArgumentParser


def main(db, congress, table, bodies):
    assert db is not None and congress in list(range(93, 116))

    if not bodies:
        url = f'https://s3.amazonaws.com/pp-projects-static/congress/bills/{congress}.zip'
        r = requests.get(url, allow_redirects=True)

        with open(f'{congress}.zip', 'wb') as f:
            f.write(r.content)
        
        with BillsToDB(congress, db, table) as bills_to_db:
            bills_to_db.extract_file(f'{congress}.zip')
            files = bills_to_db.get_filetypes()
            files = [i for i in files if 'amendments' not in i]
            bills_to_db.migrate_to_psql(files)

        os.unlink(f'{congress}.zip')
    else:
        with BillsToDB(congress, db, table) as bills_to_db:
            bills_to_db.add_bill_bodies()
            

if __name__ == '__main__':
    parser = ArgumentParser(
        description='Push data from ProPublica on bills to Postgres'
    )
    parser.add_argument('--db', help='Name of DB', type=str)
    parser.add_argument('--congress', help='Congress Number', type=int)
    parser.add_argument('--table', help='Table name', type=str, default='bills')
    parser.add_argument('--bodies', help='Scrape congressional bodies', 
                        action='store_true', default=False)
    args = parser.parse_args()

    main(args.db, args.congress, args.table, args.bodies)
