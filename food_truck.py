# all the imports
from flask import Flask, request, session, g, redirect, url_for, \
        render_template
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from geoalchemy2.functions import ST_Distance_Sphere
import urllib2
import json
from contextlib import closing
from geopy.geocoders import GoogleV3


# create application

app = Flask(__name__)
app.config.from_object('config')


# initialize database

engine = create_engine(app.config['DATABASE_URI'])

Base = declarative_base()

Session = sessionmaker(bind=engine)

class Truck(Base):
    __tablename__ = 'trucks'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = Column(String)
    location = Column(Geometry('POINT'))

    def __init__(self, name, address, lon, lat):
        self.name = name
        self.address = address
        self.location = WKTElement('POINT(%s %s)' % (lon, lat))

def init_db():
    with closing(Session()) as session:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        trucks = json.loads(urllib2.urlopen(app.config['DATA_URL']).read())
        trucks = [d for d in json.loads(urllib2.urlopen(
            app.config['DATA_URL']).read()) if
            all(k in d for k in ('applicant', 'address', 'latitude',
            'longitude')) and d['status'] == 'APPROVED']
        session.add_all([Truck(truck['applicant'], truck['address'],
            truck['longitude'], truck['latitude']) for truck in trucks])
        session.commit()

init_db()


# initialize geocoder

geolocator = GoogleV3()


# each request gets its own database session

@app.before_request
def before_request():
    g.db_session = Session()

@app.teardown_request
def teardown_request(exception):
    db_session = getattr(g, 'db_session', None)
    if db_session is not None:
        db_session.close()


# define view functions

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/search')
def search():
    loc = geolocator.geocode(request.args['address'])
    loc_pt_str = 'POINT(%s %s)' % (loc.longitude, loc.latitude)
    dist = float(request.args['dist'])
    session = g.db_session
    trucks_dist = session.query(Truck, Truck.name, Truck.address,\
            (func.ST_Distance_Sphere(Truck.location, loc_pt_str) / 1609.34).\
            label('distance')).subquery()
    trucks_close_q = session.query(trucks_dist).\
            filter(trucks_dist.c.distance <= dist).\
            order_by(trucks_dist.c.distance)
    trucks_close = trucks_close_q.all()
    trucks_close_count = trucks_close_q.count()
    return render_template('search.html', trucks=trucks_close,
            count=trucks_close_count)


# run application

if __name__ == '__main__':
    app.run()
