import argparse
import os.path
from pathlib import Path
import re
import zipfile

import pandas
import sqlalchemy as sql

from taxiclass import Ride, Base

parser = argparse.ArgumentParser()
parser.add_argument('file', metavar='FILE', type=Path)
parser.add_argument('-c', '--chunksize', type=int, default=1000)
parser.add_argument('--upperlimit', type=int, default=None)
parser.add_argument('--delete', action='store_true',
    help='Before inserting, delete all rows in the database from this file.')
    
def get_password():
    home = Path(os.path.expanduser('~'))
    with (home / '.pgpass').open('r') as f:
        for line in f:
            host, port, database, user, password = line.strip().split(':')
            if database == 'taxis' and user == 'wendell':
                return password
                
    return None

def delete_all(session, csvnum, chunk_size):
    ix = 0
    nleft = (session
        .query(Ride)
        .filter(Ride.csvnum==csvnum, Ride.rownum >= ix)
        .count()
    )
    print('Deleting', nleft, 'rows.')
    while nleft > 0:
        print('Deleting at ', ix, 'left:', nleft)
        to_del = (session
            .query(Ride)
            .filter(Ride.csvnum==csvnum, Ride.rownum < ix + chunk_size)
            .delete()
        )
        session.commit()
        ix += chunk_size
        nleft = (session
            .query(Ride)
            .filter(Ride.csvnum==csvnum, Ride.rownum >= ix)
            .count()
        )

def path_to_csvnum(path):
    match = re.match('trip_data_([0-9]*).csv.zip', path.name)
    n_str, = match.groups()
    return int(n_str)

def chunk_iter(path, csvnum, chunk_size):
    with path.open('rb') as fd:
        unzipped = zipfile.ZipFile(fd)
        fname, = unzipped.namelist()

        with unzipped.open(fname) as fd:
            for chunk_no, chunk in enumerate(pandas.read_csv(fd, chunksize=chunk_size, parse_dates=[5,6]),0): #dtype=dtypes), 1):
                yield csvnum, chunk_no, chunk

def to_rides(chunks, session, chunk_size):
    for csvnum, chunk_no, chunk in chunks:
        start_ix, end_ix = chunk_no * chunk_size, (chunk_no + 1) * chunk_size
        nums = (session.query(Ride.rownum)
            .filter_by(csvnum=csvnum)
            .filter(start_ix <= Ride.rownum, Ride.rownum < end_ix).all()
        )
        to_insert_indices = set(chunk.index + start_ix) - {n.rownum for n in nums}
        rides = []
        for ix in to_insert_indices:
            #datum = chunk.iloc[ix - start_ix]
            kw = {k: Ride.translator[k](getattr(chunk, k)[ix - start_ix]) for k in chunk.columns}
            ride_dict = dict(csvnum=csvnum, rownum=int(ix), **kw)
            rides.append(ride_dict)
        if len(rides) > 0:
            yield rides

def insert_rides(ride_dicts, session):
    print('Inserting', len(ride_dicts), 'rows.')
    try:
        for ride_dict in ride_dicts:
            session.add(Ride(**ride_dict))
        last_row = max([d['rownum'] for d in ride_dicts])
        session.commit()
    except:
        session.rollback()
        raise
    return len(ride_dicts), last_row

def run(args):
    password = get_password()
    if password is None:
        print('No password found in .pgpass.', file=sys.stderr())
        return 2
    
    try:
        csvnum = path_to_csvnum(args.file)
    except TypeError:
        print("Could not get csvnum from file.", file=sys.stderr())
        return 3
    
    url = 'postgresql://{user}:{password}@{host}/{database}'.format(
            host='localhost', 
            database='taxis', user='wendell', 
            password=password)
    engine = sql.create_engine(url)
    connection = engine.connect()
    Base.metadata.create_all(engine)
    
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()
    if args.delete:
        delete_all(session, csvnum, chunk_size=args.chunksize)
    
    chunks = chunk_iter(args.file, csvnum, chunk_size=args.chunksize)
    chunks_finished, rows_inserted = 0, 0
    for ride_set in to_rides(chunks, session, chunk_size=args.chunksize):
        n_rows, last_row = insert_rides(ride_set, session)
        chunks_finished += 1
        rows_inserted += n_rows
        print('Chunk {}, inserted {} / {} rows, last # {}'.format(
            chunks_finished, n_rows, rows_inserted, last_row))
        if args.upperlimit is not None and rows_inserted >= args.upperlimit:
            break
    Session.close_all()
    print('Finished.')

if __name__ == '__main__':
    args = parser.parse_args()
    run(args)
