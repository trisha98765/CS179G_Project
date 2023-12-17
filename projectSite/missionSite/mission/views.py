from django.shortcuts import render
from django.views.decorators.csrf import csrf_protect
import mysql.connector
from mpl_toolkits.basemap import Basemap
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from plotly.offline import plot
# Create your views here.

#def index(request):
#    return HttpResponse("Hello world. You're at the mission index.")
@csrf_protect
def index(request):
    dbConnection = mysql.connector.connect(user = "Group4", password = "Group4!", database = "Project")
    df1 = pd.read_sql("SELECT * FROM AverageFlightPrices", dbConnection)
    df2 = pd.read_sql("SELECT * FROM StartingFlights", dbConnection)
    df3 = pd.read_sql("SELECT * FROM TravelDistance", dbConnection)
    df4 = pd.read_sql("SELECT * FROM MinMaxFlights", dbConnection)
    df4 = df4.rename(columns = {"DestinationAirport" : "StartingAirport"})
    df4 = df4.drop(columns = ["AverageBasefare"])
    finalDF = pd.merge(df1, df2, "left")
    finalDF = pd.merge(finalDF, df3, "left")
    finalDF = pd.merge(finalDF, df4, "left", on = "StartingAirport")
    df2l = pd.read_sql("SELECT * FROM TwoLegFlights", dbConnection)
    df3l = pd.read_sql("SELECT * FROM ThreeLegFlights", dbConnection)
    mean2l = np.mean(df2l["BaseFare"])
    mean3l = np.mean(df3l["BaseFare"])
    data = finalDF
    finalDF["OneLayoverDifference"] = round(finalDF["AverageBasefare"] - mean2l, 2)
    finalDF["TwoLayoverDifference"] = round(finalDF["AverageBasefare"] - mean3l, 2)
    finalDF["OneLayoverTruth"] = finalDF["OneLayoverDifference"].apply(lambda x: "Layover Cheaper" if x >= 0 else "Layovers More Expensive")
    finalDF["TwoLayoverTruth"] = finalDF["TwoLayoverDifference"].apply(lambda x: "Layover Cheaper" if x >= 0 else "Layovers More Expensive")
    finalDF["OneLayoverDifference"] = abs(finalDF["OneLayoverDifference"])
    finalDF["TwoLayoverDifference"] = abs(finalDF["TwoLayoverDifference"])
    finalDF = finalDF.astype(str)
    #finalDF = finalDF.to_html()
    #dbCursor = dbConnection.cursor()
    #dbCursor.execute("USE Project;")
    #dbCursor.execute("SELECT * FROM AverageFlightPrices;")

    #return HttpResponse(str(finalDF.dtypes))
    if request.method == "POST":
        start = request.POST.get("stAir", None)
        dest = request.POST.get("endAir", None)
        print(start)
        print(dest)
    else:
        start = ""
        dest = ""

    # initialise
    df_airports = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/2011_february_us_airport_traffic.csv')
    #df_airports.head()

    airlines = ['ATL','BOS','CLT', 'DEN','DFW','DTW','EWR','IAD','JFK','LAX','LGA','MIA','OAK','ORD','PHL', 'SFO']

    df_airports = df_airports[df_airports['iata'].isin(airlines)]
    

    df_flight_paths = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/2011_february_aa_flight_paths.csv')
    #df_flight_paths.head()
    df_airports = df_airports.sort_values("iata")
    #return HttpResponse(str(df_airports["iata"])) 

    fig = go.Figure()

    fig.add_trace(go.Scattergeo(
        locationmode = 'USA-states',
        lon = df_airports['long'],
        lat = df_airports['lat'],
        hoverinfo = 'text',
        text = "Airport: " + finalDF["StartingAirport"] + "<br>" + "Avg Basefare: $" + finalDF["AverageBasefare"] + "<br>" + "Avg Cost of Flight: $" + finalDF["AverageCost"] + "<br>" + "Avg Amount of Flights per Day: " + finalDF["AverageFlightsPerDay"] + "<br>" + "Avg Travel Distance: " + finalDF["AverageTravelDistance"] + " Miles" + "<br>" + "Min Basefare: $" + finalDF["MinBasefare"] + "<br>" + "Max Basefare: $" + finalDF["MaxBasefare"] + "<br>" + "Avg Layover Basefare Difference: $" + finalDF["OneLayoverDifference"] + ", " + finalDF["OneLayoverTruth"] + "<br>" + "Avg 2-Layover Basefare Difference: $" + finalDF["TwoLayoverDifference"] + ", " + finalDF["TwoLayoverTruth"],
        mode = 'markers',
        marker = dict(
            size = 4,
            color = 'rgb(0, 159, 255)',
            line = dict(
                width = 3,
                color = 'rgb(0, 159, 255)' )
        )
        ))
    if (((finalDF["StartingAirport"].eq(start)).any() == True) and ((finalDF["StartingAirport"].eq(dest)).any() == True)):
        query1l = pd.read_sql("SELECT BaseFare FROM TwoLegFlights WHERE StartingAirport = '%s' AND DestinationAirport = '%s'" % (start, dest), dbConnection)

        query2l = pd.read_sql("SELECT BaseFare FROM ThreeLegFlights WHERE StartingAirport = '%s' AND DestinationAirport = '%s'" % (start, dest), dbConnection)

        difference1 = finalDF[finalDF["StartingAirport"] == start]
        difference1 = finalDF["OneLayoverDifference"].iloc[0]
        difference2 = finalDF[finalDF["StartingAirport"] == dest]
        difference2 = finalDF["TwoLayoverDifference"].iloc[0]

        startdf = df_airports[df_airports["iata"] == start]
        destdf = df_airports[df_airports["iata"] == dest]

        fig.add_trace(go.Scattergeo(
            locationmode = 'USA-states',
            lon = [startdf['long'].iloc[0], destdf['long'].iloc[0]],
            lat = [startdf['lat'].iloc[0], destdf['lat'].iloc[0]],
            hoverinfo = 'text',
            text = "Average Layover Basefare Difference Between " + start + " and " + dest + ": $" + difference1 + "<br>" + "Average 2-Layover Basefare Difference Between " + start + " and " + dest + ": $" + difference2,
            mode = 'lines',
            line = dict(width = 3, color = 'red')   
        ))
    else:
        print("didnt work")

    fig.update_layout(
        title_text = 'Flight Analysis Map',
        showlegend = False,
        geo = dict(
            scope = 'north america',
            projection_type = 'azimuthal equal area',
            showland = True,
            landcolor = 'rgb(243, 243, 243)',
            countrycolor = 'rgb(204, 204, 204)',
        ),
        height=800, 
        width=1400, 
    )    
    plotly_plot_obj = plot({'data': fig}, output_type='div')
    return render(request, 'index.html', {'plotly_plot_obj': plotly_plot_obj}) 
