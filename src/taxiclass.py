import sqlalchemy as sql
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class Ride(Base):
    __tablename__ = 'rides'
    csvnum = sql.Column(sql.Integer, primary_key=True)
    rownum = sql.Column(sql.Integer, primary_key=True)
    medallion = sql.Column(sql.String)
    hack_license = sql.Column(sql.String)
    vendor_id = sql.Column(sql.String)
    rate_code = sql.Column(sql.Integer)
    store_and_fwd_flag = sql.Column(sql.String)
    pickup_datetime = sql.Column(sql.DateTime, primary_key=True)
    dropoff_datetime = sql.Column(sql.DateTime)
    passenger_count = sql.Column(sql.Integer)
    trip_time_in_secs = sql.Column(sql.Float)
    trip_distance = sql.Column(sql.Float)
    pickup_longitude = sql.Column(sql.Float)
    pickup_latitude = sql.Column(sql.Float)
    dropoff_longitude = sql.Column(sql.Float)
    dropoff_latitude = sql.Column(sql.Float)
    
    translator = dict(
        medallion=str,
        hack_license=str,
        vendor_id=str,
        rate_code=int,
        store_and_fwd_flag=str,
        pickup_datetime=lambda d: d.to_datetime(),
        dropoff_datetime=lambda d: d.to_datetime(),
        passenger_count=int,
        trip_time_in_secs=float,
        trip_distance=float,
        pickup_longitude=float,
        pickup_latitude=float,
        dropoff_longitude=float,
        dropoff_latitude=float,
    )

    def __repr__(self):
        return "<Ride(pickup='%s', time='%s', distance='%s')>" % (
            self.pickup_datetime, self.trip_time_in_secs, self.trip_distance)
