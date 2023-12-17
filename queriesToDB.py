#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Import libraries
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, month, year, format_number, col, count, round
import time
import mysql.connector

# In[2]:


# Start a Spark session
from pyspark.sql import SparkSession
# change number of workers in master argument below (1,2,4,*)
spark = SparkSession.builder \
    .config("spark.jars", "/usr/share/java/mysql-connector-j-8.0.31.jar") \
    .master("local[*]").appName("AirportQueries").getOrCreate()
# Read in data
file_path = 'subset_3.csv'
data = spark.read.csv(file_path, header=True)

# Create Database Connection
databaseConnection = mysql.connector.connect(user = "Group4", password = "Group4!")
databaseCursor = databaseConnection.cursor()
databaseCursor.execute("USE Project;")

# Average baseFare during the summer months (June, July, August 2022) per startingAirport

# In[3]:


# convert 'flightDate' column type to date
data = data.withColumn('flightDate', data['flightDate'].cast('date'))
# filter flights for June, July, August (summer months) in 2022
data_filtered = data.filter((month(data.flightDate).isin(6, 7, 8)) & (year(data.flightDate) == 2022))
# group by startingAirport and find the average baseFare
avg_flight_prices = data_filtered.groupBy("startingAirport") \
    .agg(format_number(avg("baseFare"), 2).alias("average_baseFare"))
# result
avg_flight_prices.show()

# Store Result in AverageBasefare table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS AverageFlightPrices(StartingAirport CHAR(3), AverageBasefare FLOAT);")
databaseCursor.execute("ALTER TABLE AverageFlightPrices ADD UNIQUE INDEX(StartingAirport, AverageBasefare);")

records = avg_flight_prices.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query
for record in records: 
    stringRecord = "('%s', %f)" % (str(record.__getitem__("startingAirport")), float(record.__getitem__("average_baseFare")))
    databaseCursor.execute("INSERT IGNORE INTO AverageFlightPrices(StartingAirport, AverageBasefare) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM AverageFlightPrices")
print(databaseCursor.fetchall())


# Average baseFare when startingAirport = 'SFO' and destinationAirport = 'LAX'

# In[4]:


average_basefare = data.filter((data.startingAirport == 'SFO') & (data.destinationAirport == 'LAX'))\
    .select(avg(data.baseFare).alias('average_baseFare'))
average_basefare.show()

# Store Result in SFOtoLAX table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS SFOtoLAX(AverageBasefare FLOAT);")
databaseCursor.execute("ALTER TABLE SFOtoLAX ADD UNIQUE INDEX(AverageBasefare);")

records = average_basefare.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query
stringRecord = "(%f)" % (float(records[0].__getitem__("average_baseFare")))
databaseCursor.execute("INSERT IGNORE INTO SFOtoLAX(AverageBasefare) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM SFOtoLAX")
print(databaseCursor.fetchall())


# Average number of flights and average cost per starting airport

# In[5]:


# average number of flights per starting airport
avg_flights_per_day = data.groupBy("startingAirport", "flightDate") \
    .agg(count("legId").alias("numFlightsPerDay")) \
    .groupBy("startingAirport") \
    .agg(round(avg("numFlightsPerDay")).alias("avgFlightsPerDay"))
# average cost per starting airport rounded to two decimal places
avg_cost_per_airport = data.groupBy("startingAirport") \
    .agg(round(avg("baseFare"), 2).alias("avgCost"))
# join the two results based on startingAirport
result = avg_flights_per_day.join(avg_cost_per_airport, "startingAirport")
# result
result.show()

# Store Result in StartingFlights table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS StartingFlights(StartingAirport CHAR(3), AverageFlightsPerDay FLOAT, AverageCost FLOAT);")
databaseCursor.execute("ALTER TABLE StartingFlights ADD UNIQUE INDEX(StartingAirport, AverageFlightsPerDay, AverageCost);")

records = result.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query
for record in records: 
    stringRecord = "('%s', %f, %f)" % (str(record.__getitem__("startingAirport")), float(record.__getitem__("avgFlightsPerDay")), float(record.__getitem__("avgCost")))
    databaseCursor.execute("INSERT IGNORE INTO StartingFlights(StartingAirport, AverageFlightsPerDay, AverageCost) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM StartingFlights")
print(databaseCursor.fetchall())


# Airport with the maximum baseFare in the dataset

# In[6]:


# group by destinationAirport and find the maximum baseFare
max_basefare_per_destination = data.groupBy("destinationAirport").agg(max("baseFare").alias("max_baseFare"))
# destination(s) with the highest baseFare
max_expensive_destinations = max_basefare_per_destination.filter(max_basefare_per_destination.max_baseFare\
     == max_basefare_per_destination.agg(max("max_baseFare")).collect()[0][0])
# result
max_expensive_destinations.show()


# Store Result in MaxAirportBasefare table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS MaxAirportBasefare(DestinationAirport CHAR(3), MaxBasefare FLOAT);")
databaseCursor.execute("ALTER TABLE MaxAirportBasefare ADD UNIQUE INDEX(DestinationAirport, MaxBasefare);")

records = max_expensive_destinations.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query 
stringRecord = "('%s', %f)" % (str(records[0].__getitem__("destinationAirport")), float(records[0].__getitem__("max_baseFare")))
databaseCursor.execute("INSERT IGNORE INTO MaxAirportBasefare(DestinationAirport, MaxBasefare) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM MaxAirportBasefare")
print(databaseCursor.fetchall())


# Airport with the minimum baseFare in the dataset

# In[7]:


# group by destinationAirport and find the minimum baseFare
min_basefare_per_destination = data.groupBy("destinationAirport").agg(min("baseFare").alias("min_baseFare"))
# destination(s) with the lowet baseFare
min_expensive_destinations = min_basefare_per_destination.filter(min_basefare_per_destination.min_baseFare == \
    min_basefare_per_destination.agg(min("min_baseFare")).collect()[0][0])
# result
min_expensive_destinations.show()

# Store Result in MinAirportBaseFare table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS MinAirportBaseFare(DestinationAirport CHAR(3), MinBasefare FLOAT);")
databaseCursor.execute("ALTER TABLE MinAirportBaseFare ADD UNIQUE INDEX(DestinationAirport, MinBasefare);")

records = min_expensive_destinations.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query
for record in records: 
    stringRecord = "('%s', %f)" % (str(record.__getitem__("destinationAirport")), float(record.__getitem__("min_baseFare")))
    databaseCursor.execute("INSERT IGNORE INTO MinAirportBaseFare(DestinationAirport, MinBaseFare) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM MinAirportBaseFare")
print(databaseCursor.fetchall())


# Least expensive flight, most expensive flight, average flight cost per destination airport

# In[13]:


# group by destinationAirport, find the maximum baseFare
max_min_avg_expensive_destinations = data.groupBy("destinationAirport").agg(min("baseFare").alias("min_baseFare"), \
    max("baseFare").alias("max_baseFare"), format_number(avg("baseFare"),2).alias("avg_baseFare"))
# show the result
max_min_avg_expensive_destinations.show()

# Store Result in MinMaxFlights table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS MinMaxFlights(DestinationAirport CHAR(3), MinBasefare FLOAT, MaxBasefare FLOAT, AverageBasefare FLOAT);")
databaseCursor.execute("ALTER TABLE MinMaxFlights ADD UNIQUE INDEX(DestinationAirport, MinBasefare, MaxBasefare, AverageBasefare);")

records = max_min_avg_expensive_destinations.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query
for record in records: 
    stringRecord = "('%s', %f, %f, %f)" % (str(record.__getitem__("destinationAirport")), float(record.__getitem__("min_baseFare")), float(record.__getitem__("max_baseFare")), float(record.__getitem__("avg_baseFare")))
    databaseCursor.execute("INSERT IGNORE INTO MinMaxFlights(DestinationAirport, MinBasefare, MaxBasefare, AverageBaseFare) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM MinMaxFlights")
print(databaseCursor.fetchall())


# For each airline, show the most expensive flight and the corresponding start and destination airport. Only show the flights with one leg (meaning no layovers). This was done because some flights involved multiple different airlines.

# In[14]:


# filter out one leg flights only
filtered_data_oneleg = data.filter(~col("segmentsAirlineName").contains("||"))
# find the most expensive flight, grouped by airline name
most_expensive_per_airline = filtered_data_oneleg.groupBy("segmentsAirlineName") \
    .agg(max("baseFare").alias("maxBaseFare"))
# retrieve the rows with the highest baseFare for each airline and remove duplicates
expensive_flights_info_oneleg = filtered_data_oneleg.join(most_expensive_per_airline.withColumnRenamed("segmentsAirlineName", "max_segmentsAirlineName"),
                                            (filtered_data_oneleg.baseFare == most_expensive_per_airline.maxBaseFare) &
                                            (filtered_data_oneleg.segmentsAirlineName == most_expensive_per_airline.segmentsAirlineName),
                                            "inner") \
    .select("segmentsAirlineName", "startingAirport", "destinationAirport", "baseFare").dropDuplicates(["segmentsAirlineName"]) 
# show the result
expensive_flights_info_oneleg.show()

# Store Result in OneLegFlights table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS OneLegFlights(AirlineName TEXT, StartingAirport CHAR(3), DestinationAirport CHAR(3), BaseFare FLOAT);")
databaseCursor.execute("ALTER TABLE OneLegFlights ADD UNIQUE INDEX(AirlineName, StartingAirport, DestinationAirport, Basefare);")

records = expensive_flights_info_oneleg.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query
for record in records: 
    stringRecord = "('%s', '%s', '%s', %f)" % (str(record.__getitem__("segmentsAirlineName")), str(record.__getitem__("startingAirport")), str(record.__getitem__("destinationAirport")), float(record.__getitem__("baseFare")))
    databaseCursor.execute("INSERT IGNORE INTO OneLegFlights(AirlineName, StartingAirport, DestinationAirport, baseFare) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM OneLegFlights")
print(databaseCursor.fetchall())


# Flights with two legs, could involve the same airline twice or two different airlines.

# In[15]:


# airline name column contains '||' once means its a two leg flight
filtered_data_twoleg = data.filter(col("segmentsAirlineName").rlike(r'^[^|]*\|{2}[^|]*$'))
# find most expensive flight, grouped by airline
most_expensive_per_airline = filtered_data_twoleg.groupBy("segmentsAirlineName") \
    .agg(max("baseFare").alias("maxBaseFare"))
# retrieve the rows with the highest baseFare for each airline and remove duplicates
expensive_flights_info_twoleg = filtered_data_twoleg.join(most_expensive_per_airline.withColumnRenamed("segmentsAirlineName", "max_segmentsAirlineName"),
                                            (filtered_data_twoleg.baseFare == most_expensive_per_airline.maxBaseFare) &
                                            (filtered_data_twoleg.segmentsAirlineName == most_expensive_per_airline.segmentsAirlineName),
                                            "inner") \
    .select("segmentsAirlineName", "startingAirport", "destinationAirport", "baseFare").dropDuplicates(["segmentsAirlineName"]) 

expensive_flights_info_twoleg.show()

# Store Result in TwoLegFlights table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS TwoLegFlights(AirlineName TEXT, StartingAirport CHAR(3), DestinationAirport CHAR(3), BaseFare FLOAT);")
databaseCursor.execute("ALTER TABLE TwoLegFlights ADD UNIQUE INDEX(AirlineName, StartingAirport, DestinationAirport, Basefare);")

records = expensive_flights_info_twoleg.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query
for record in records: 
    stringRecord = "('%s', '%s', '%s', %f)" % (str(record.__getitem__("segmentsAirlineName")), str(record.__getitem__("startingAirport")), str(record.__getitem__("destinationAirport")), float(record.__getitem__("baseFare")))
    databaseCursor.execute("INSERT IGNORE INTO TwoLegFlights(AirlineName, StartingAirport, DestinationAirport, baseFare) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM TwoLegFlights")
print(databaseCursor.fetchall())


# In[16]:


# if the column contains "||" twice, it's a three leg flight
filtered_data_threeleg = data.filter(col("segmentsAirlineName").rlike(r'^([^|]*\|{2}[^|]*){2}$'))
# find the most expensive, group by airline
most_expensive_per_airline = filtered_data_threeleg.groupBy("segmentsAirlineName") \
    .agg(max("baseFare").alias("maxBaseFare"))
# retrieve the rows with the highest baseFare for each airline and remove duplicates
expensive_flights_info_threeleg = filtered_data_threeleg.join(most_expensive_per_airline.withColumnRenamed("segmentsAirlineName", "max_segmentsAirlineName"),
                                            (filtered_data_threeleg.baseFare == most_expensive_per_airline.maxBaseFare) &
                                            (filtered_data_threeleg.segmentsAirlineName == most_expensive_per_airline.segmentsAirlineName),
                                            "inner") \
    .select("segmentsAirlineName", "startingAirport", "destinationAirport", "baseFare").dropDuplicates(["segmentsAirlineName"]) 

expensive_flights_info_threeleg.show()

# Store Result in ThreeLegFlights table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS ThreeLegFlights(AirlineName TEXT, StartingAirport CHAR(3), DestinationAirport CHAR(3), BaseFare FLOAT);")
databaseCursor.execute("ALTER TABLE ThreeLegFlights ADD UNIQUE INDEX(AirlineName, StartingAirport, DestinationAirport, Basefare);")

records = expensive_flights_info_threeleg.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query
for record in records: 
    stringRecord = "('%s', '%s', '%s', %f)" % (str(record.__getitem__("segmentsAirlineName")), str(record.__getitem__("startingAirport")), str(record.__getitem__("destinationAirport")), float(record.__getitem__("baseFare")))
    databaseCursor.execute("INSERT IGNORE INTO ThreeLegFlights(AirlineName, StartingAirport, DestinationAirport, baseFare) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM ThreeLegFlights")
print(databaseCursor.fetchall())


# In[12]:


#  average total travel distance per starting airport
avg_distance_per_starting_airport = data.groupBy("startingAirport") \
    .agg(avg("totalTravelDistance").alias("avgTotalTravelDistance"))
# sort by highest average total travel distance
sorted_avg_distance = avg_distance_per_starting_airport.orderBy("avgTotalTravelDistance", ascending=False)
# result
sorted_avg_distance.show()

# Store Result in TravelDistance table within Project Database
databaseCursor.execute("CREATE TABLE IF NOT EXISTS TravelDistance(StartingAirport CHAR(3), AverageTravelDistance FLOAT);")
databaseCursor.execute("ALTER TABLE TravelDistance ADD UNIQUE INDEX(StartingAirport, AverageTravelDistance);")

records = sorted_avg_distance.collect() # Get list of each row
# For each row, get each column value, form a query string with the values and concat the string to the sql query
for record in records: 
    stringRecord = "('%s', %f)" % (str(record.__getitem__("startingAirport")), float(record.__getitem__("avgTotalTravelDistance")))
    databaseCursor.execute("INSERT IGNORE INTO TravelDistance(StartingAirport, AverageTravelDistance) VALUES " + stringRecord + ";")

databaseCursor.execute("FLUSH TABLES;")

# Verify
databaseCursor.execute("SELECT * FROM TravelDistance")
print(databaseCursor.fetchall())
