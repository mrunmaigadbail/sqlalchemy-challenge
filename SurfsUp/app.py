# Import the dependencies.
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, text
import numpy as np
import pandas as pd
import datetime as dt

from flask import Flask, jsonify



#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
conn = engine.connect()

session = Session(engine)


# Find the first date in the data set.
   
first_date = session.query(Measurement.date).order_by(Measurement.date.asc()).first()
first_date_str = first_date[0]
db_start_date = dt.date.fromisoformat(first_date_str)



# Find the most recent date in the data set.
recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

# Calculate the date one year from the last date in data set.
last_date_str = recent_date[0]
db_end_date = dt.date.fromisoformat(last_date_str)
one_year_ago = db_end_date - dt.timedelta(days=365)
one_year_ago

    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    # List the stations and their counts in descending order.
station_counts = session.query(Measurement.station, func.count(Measurement.date)).\
                        group_by(Measurement.station).\
                        order_by(func.count(Measurement.date).desc()).\
                        all()
most_active_station = station_counts[0][0]
#################################################
# Flask Setup
#################################################

app = Flask(__name__)


#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs</br>"
        f"Enter start date between {db_start_date} to {db_end_date} in YYYY-MM-DD format</br>"
        f"/api/v1.0/<start></br>"
        f"Enter start and end date between {db_start_date} to {db_end_date} in YYYY-MM-DD format</br>"
        f"/api/v1.0/<start>/<end></br></br><br>"
        f"################################################</br>"
        f"/api/v1.0/precipitation_sql<br/>"
        f"/api/v1.0/stations_sql<br/>"
        f"/api/v1.0/min_max_avg_tobs</br>"
        f"/api/v1.0/min_max_avg_tobs_sql</br>"
        f"/api/v1.0/tobs_sql</br>"

    )

@app.route("/api/v1.0/precipitation")
def precipitation():

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago).all() 
    #results
    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    df = pd.DataFrame(results, columns=['date', 'precipitation'])

    # Sort the dataframe by date
    df_sorted = df.sort_values(by='date').reset_index(drop=True)

    data = df_sorted.to_dict(orient="records")
    return(jsonify(data))



@app.route("/api/v1.0/stations")
def stations():
    #list of stations from the dataset and number of counts
    df_stations = pd.DataFrame(station_counts, columns=['station', '# counts'])
    data = df_stations.to_dict(orient="records")
    return(jsonify(data))



@app.route("/api/v1.0/tobs")
def tobs():
    

    # Using the most active station id
# Query the last 12 months of temperature observation data for this station 
    results = session.query(Measurement.date, Measurement.tobs).\
                filter(Measurement.date >= one_year_ago).\
                filter(Measurement.station == most_active_station).\
                all() 
    df_tobs = pd.DataFrame(results, columns=['Date','tobs'])
    data = df_tobs.to_dict(orient="records")
    return(jsonify(data))



@app.route("/api/v1.0/<start>")
def start_date(start):
    #calculating min, avg, and max for all the dates greater than or equal to the specified start date
    start_date= dt.datetime.strptime(start, '%Y-%m-%d')
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).all()
    df_start = pd.DataFrame(results, columns=['min','avg','max'])
    data = df_start.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start,end):
     #calculating min, avg, and max for all the dates between the specified start date and end date
    start_date= dt.datetime.strptime(start, '%Y-%m-%d')
    end_date= dt.datetime.strptime(end,'%Y-%m-%d')

    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    df_start_end = pd.DataFrame(results, columns=['min','avg','max'])
    data = df_start_end.to_dict(orient="records")
    return jsonify(data)


@app.route("/api/v1.0/precipitation_sql")
def precipitation_sql():
   
    
    # Perform a query to retrieve the data and precipitation scores
    query = text("""
            Select date, prcp as precipitation
            from measurement
            where date >= '2016-08-23'
            order by date asc
            """)
    df_sql = pd.read_sql((query), con=conn)
    data = df_sql.to_dict(orient="records")
    
    return(jsonify(data))


@app.route("/api/v1.0/stations_sql")
def stations_sql():
     #list of stations from the dataset and number of counts
    query=text(""" select station, count(date) as '# counts'
            from measurement
            group by station
            order by count(date) desc""")
    station_counts_sql = conn.execute(query).all()
    
 
    df_stations_sql = pd.read_sql((query), con=conn)
    data = df_stations_sql.to_dict(orient="records")
   
    return(jsonify(data))


@app.route("/api/v1.0/min_max_avg_tobs")
def min_max_avg_tobs():
  

    # Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
    min_max_avg = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
                filter(Measurement.station==most_active_station).\
                all()

    df_min_max_avg = pd.DataFrame(min_max_avg, columns=['min', 'max', 'avg'])

    data = df_min_max_avg.to_dict(orient="records")
    return(jsonify(data))


@app.route("/api/v1.0/min_max_avg_tobs_sql")
def min_max_avg_tobs_sql():
    
    # Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
    query = text("""select min(tobs), max(tobs), avg(tobs)
            from measurement
            where station = 'USC00519281'""")
    min_max_avg_sql = pd.read_sql((query), con=conn)
    data = min_max_avg_sql.to_dict(orient="records")
    

    return(jsonify(data))
    
# Using the most active station id
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram



@app.route("/api/v1.0/tobs_sql")
def tobs_sql():
   

    # Query the last 12 months of temperature observation data for this station 
    query = text("""select date, tobs
                    from measurement
                    where station ='USC00519281' and date >= '2016-08-23' """)
    df_tobs_sql = pd.read_sql((query), con=conn)
  
    data = df_tobs_sql.to_dict(orient="records")
 
    return(jsonify(data))




session.close()

if __name__ == '__main__':
    app.run(debug=True)
