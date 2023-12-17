import pandas as pd
import zipfile

PREFIX = 'archive' #path to .zip
chunk_list = [] #list that holds
chunk_size = 5000 #size to chunk to be read in, so memory isn't overloaded
columns = ['legId', 'searchDate', 'flightDate', 'startingAirport', 'destinationAirport', 
           'isRefundable', 'isNonStop', 'baseFare', 'totalFare', 'seatsRemaining', 'travelDuration', 'totalTravelDistance','segmentsAirlineName'] #list of relevant columns to pull out of csv
with zipfile.ZipFile(f"{PREFIX}.zip", "r") as zf:
    for chunk in pd.read_csv(zf.open(f"itineraries.csv"), chunksize=chunk_size, usecols = columns):
        chunk_list.append(chunk.iloc[::5]) #appends chunk of .csv into list
        df = pd.concat(chunk_list) #merges into new Dataframe
        print((df.memory_usage().sum()/(10**9)), " Gigabytes")
        if(df.memory_usage().sum()/(10**9)) > 1: #checks current memory storage; breaks once 1 Gb is reached
            break
df.to_csv('itineraries.csv', index = False) #exporting as csv //index = False means the row number won't print
#print(df.head())
#print(df.iloc[-1]) //prints last element in dataframe