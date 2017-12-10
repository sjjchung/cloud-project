
# coding: utf-8

# ### Data From ###
# ### https://www.transtats.bts.gov/Tables.asp?DB_ID=120 ###

# ### Import plotly library###

# In[1]:

#Library for plotly

import plotly as py
import plotly.graph_objs as go

py.offline.init_notebook_mode(connected=True)


# In[2]:

import matplotlib.pyplot as plt


# In[3]:

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType


# In[4]:

AWS_BUCKET_NAME = "otp-data"
FOLDER = "1_year_data"
FORMATTER = "s3n://%s/%s/"
FILENAME = "On_Time_On_Time_Performance_%i_%i.csv"


# In[5]:

def month_year_iter(start_month, start_year, end_month, end_year):
    ym_start= 12 * start_year + start_month - 1
    ym_end= 12 * end_year + end_month
    for ym in range(ym_start, ym_end):
        y, m = divmod(ym, 12)
        yield y, m+1


# In[6]:

def returnPath(bucket, folder, beginningYear, beginningMonth, endYear, endMonth, formatter, filename):
    list = []
    for i in month_year_iter(beginningMonth,beginningYear,endMonth,endYear):
        list = list + [formatter % (bucket,folder) + filename %(i[0],i[1])]
    return list


# In[7]:

print (returnPath(AWS_BUCKET_NAME,FOLDER,2016,10,2017,9, FORMATTER, FILENAME))


# In[8]:

#Basic loading and test the validity of the dataset, should test with hashes if time permits

DFTEMP = spark.read.csv('s3n://otp-data/1_year_data/On_Time_On_Time_Performance_2017_1.csv', header=True, inferSchema=True)

assert (DFTEMP.count()==450017), "There is a problem with your dataset, counts don't match"


# In[9]:

#import all csv into a single data-frame (Takes about 10 minutes, do not run frequently)

DF=0
for files in returnPath(AWS_BUCKET_NAME,FOLDER,2016,10,2017,9, FORMATTER, FILENAME):
    if (DF==0):
        DF = spark.read.csv(files, header=True, inferSchema=True)
    else:
        DF = DF.unionAll(spark.read.csv(files, header=True, inferSchema=True))

assert (DF.count()==5660970), "There is a problem with your dataset, counts don't match"


# In[10]:

#Create smaller dataframes for better testing, one for a random sample of roughly a month's data 0.083 ~= 1/12, the other using data from January 2017, note that for sample the numbers might not add up to the fraction exactly

smallDF = DF.sample(False, 0.083)
monthDF = DF.filter(DF.Month == 1)
smallDF.cache()
monthDF.cache()

print smallDF.count()
print monthDF.count()


# In[11]:

full = 1
if full:
    smallDF = DF


# In[12]:

#Convert to RDD similar to the ones used in apache log and cache them, do not use fullRDD outside of production
fullRDD = DF.rdd
smallRDD = smallDF.rdd.cache()
monthRDD = monthDF.rdd.cache()


# In[13]:

#Define simple functions
def reduceAdd (a,b):
    return a+b

#For a certain key in the RDD, compute the total number of entries  
def numberOf(key,RDD):
    return RDD.map(lambda a: (a[key],1)).reduceByKey(reduceAdd).sortBy(lambda x: -x[1])

#For a certain key in the RDD, compute the portion of it's entries compared to all 
def shareOf(key,RDD):
    fieldRDD = numberOf(key,RDD)
    count = fieldRDD.map(lambda a:a[1]).reduce(reduceAdd)
    return fieldRDD.map(lambda a: (a[0],float(a[1])/count)).sortBy(lambda x: -x[1])


# # General Statistics of the dataset #
# ## Flight per carrier ##
# ## Flight per individual aircraft ##
# ## airports ##

# In[14]:

#Flight per carrier
carrierFlightsShare = shareOf("UniqueCarrier",smallRDD)
carrierFlights = numberOf("UniqueCarrier",smallRDD)


# In[15]:

#Code snipit adapted from Lab
def pie_pct_format(value):
    return '' if value < 6 else '%.0f%%' % value

fig = plt.figure(figsize=(4.5, 4.5), facecolor='white', edgecolor='white')
explode = (0.1, 0.1, 0.1, 0, 0, 0, 0, 0, 0, 0, 0, 0)  
patches, texts, autotexts = plt.pie(carrierFlightsShare.map(lambda a: a[1]).collect(), labels=carrierFlightsShare.map(lambda a: a[0]).collect(), explode=explode, autopct=pie_pct_format, shadow=True,  startangle=91)

for text, autotext in zip(texts, autotexts):
    if autotext.get_text() == '':
        text.set_text('')
plt.legend(carrierFlightsShare.map(lambda a: a[0]).collect(), loc=(0.8, -0.1), shadow=True)
pass

#Display is used here as databricks do not support native show feature
plt.show()


# In[16]:

#Flight per aircraft

#Filtering must be done as certain flights have a no tail number, it is further sorted to aid plotting
aircraftFlights = numberOf("TailNum",smallRDD).filter(lambda a: a[0] != None).sortBy(lambda x: x[1])
aircraftFlightsNoTail = numberOf("TailNum",smallRDD).filter(lambda a: a[0] == None)
totalAircraft = carrierFlights.map(lambda a: a[1]).reduce(reduceAdd)
unknownAircraft = aircraftFlightsNoTail.reduce(lambda a,b: a[1]+b[1])[1]

print("There are %i aircrafts in this dataset, with %i trips having unidentified tail numbers" % (totalAircraft, unknownAircraft))


# In[17]:

aircraft = aircraftFlights.map(lambda x: x[0]).collect()
flights = aircraftFlights.map(lambda x: x[1]).collect()

fig = plt.figure(figsize=(8,4.5), facecolor='white', edgecolor='white')
plt.axis([0, len(aircraft), 0, max(flights)])
plt.grid(b=True, which='major', axis='y')
plt.xlabel('Aircraft')
plt.ylabel('Number of flights')
plt.plot(flights)

plt.show()


# In[18]:

#Maximum and minimum for aircrafts
print aircraftFlights.takeOrdered(10, key = lambda x: x[1])
print aircraftFlights.takeOrdered(10, key = lambda x: -x[1])

#Could expand this section to investigate each airline and their distrubution, here we can see Hawaii Airlines fly the most


# In[19]:

#Fun fact

uniqueTail = smallRDD.map(lambda a: (a.TailNum)).filter(lambda a: a!=None).distinct().count()
uniqueTailAirlines =  smallRDD.map(lambda a: (a.TailNum,a.UniqueCarrier)).filter(lambda a: a[0]!=None).distinct().count()

print ('Fun fact, this shows that at least %i flights are "transfered" from one airline to another, since the same airplane is operated by more than one carrier. \nSince change in ownership usually acoompanies a change in tailnumber, this situation is quite unique' % (uniqueTailAirlines-uniqueTail))


# In[20]:

#Airports, see if can plot on map

actualFlight = smallRDD.filter(lambda a: a.Cancelled==0.0)

originFlight = numberOf("Origin",actualFlight).sortBy(lambda x: -x[1])
cancelledFlight = numberOf("Origin",smallRDD.filter(lambda a: a.Cancelled!=0.0)).sortBy(lambda x: -x[1])
delayedFlight = numberOf("Origin",actualFlight.filter((lambda a: a.DepDel15!=0.0))).sortBy(lambda x: -x[1])
delayedFlightShare = originFlight.leftOuterJoin(delayedFlight).map(lambda a: (a[0],(float((a[1][1] if a[1][1]!=None else 0))/a[1][0],a[1][0]))).sortBy(lambda x: -x[1][1])

#print (originFlight.collect())
#print (cancelledFlight.collect())
#print (delayedFlight.collect())


# In[21]:

delayByAirport = actualFlight.filter(lambda a: a.DepDelay>=15.0).map(lambda a: (a.Origin,a.DepDelay))

delayFlights = delayByAirport.map(lambda a: (a[0],1)).reduceByKey(reduceAdd)
delayMinuteByAirport = delayByAirport.reduceByKey(reduceAdd)
averageDelayByAirport = delayFlights.join(delayMinuteByAirport).map(lambda a: (a[0], float(a[1][1])/a[1][0])).join(originFlight).sortBy(lambda x: -x[1][1])

print delayFlights.take(5)
print delayMinuteByAirport.take(5)
print averageDelayByAirport.take(5)


# ### This section below is setup for plotting ###

# In[22]:

apInfoSchema = StructType([StructField('ID', IntegerType(), True),
                           StructField('Name', StringType(), True),
                           StructField('City', StringType(), True),
                           StructField('Country', StringType(), True),
                           StructField('IATA', StringType(), True),
                           StructField('ICAO', StringType(), True),
                           StructField('Lat', FloatType(), True),
                           StructField('Lon', FloatType(), True),
                           StructField('Alt', FloatType(), True),
                           StructField('Timezone', StringType(), True),
                           StructField('DST', StringType(), True),
                           StructField('TZ', StringType(), True),
                           StructField('Type', StringType(), True),
                           StructField('Source', StringType(), True)])

airportFlightSchema = StructType([StructField('IATA', StringType(), True),
                                 StructField('flight', IntegerType(), True)])


# In[23]:

#Information on airport to be joined with other DFs
apInfoFullDF=spark.read.csv('s3n://otp-data/airports.dat', header=False, schema=apInfoSchema)
apInfoDF=apInfoFullDF.drop("ID").drop("Country").drop("ICAO").drop("Alt").drop("Timezone").drop("DST").drop("TZ").drop("Type").drop("Source")
assert(apInfoDF.count()==7184), "Count does not match"


# In[24]:

def apPlot (apFlightRDD, title, scale):
    apFlightDF = sqlContext.createDataFrame(apFlightRDD,airportFlightSchema)
    apDF = apFlightDF.join(apInfoDF, "IATA", "left_outer")
    # Omitted airports not on apInfoDF
    apDF = apDF.filter(apDF.Name!="")
    apPD = apDF.toPandas()
    
    apPD['text'] = apPD['Name'] + '<br>Flights: ' + (apPD['flight']).astype(str) + '<br>IATA: ' + apPD['IATA']
    limits = [(0,2),(3,10),(11,20),(21,50),(50,3000)]
    colors = ["rgb(0,116,217)","rgb(255,65,54)","rgb(133,20,75)","rgb(255,133,27)","lightgrey"]
    airports = []

    for i in range(len(limits)):
        lim = limits[i]
        df_sub = apPD[lim[0]:lim[1]]
        ap = dict(
            type = 'scattergeo',
            locationmode = 'USA-states',
            lon = df_sub['Lon'],
            lat = df_sub['Lat'],
            text = df_sub['text'],
            marker = dict(
                size = df_sub['flight']/scale,
                color = colors[i],
                line = dict(width=0.5, color='rgb(40,40,40)'),
                sizemode = 'area'
            ),
            name = '{0} - {1}'.format(lim[0],lim[1]) )
        airports.append(ap)

        layout = dict(
            title = title,
            showlegend = True,
            geo = dict(
                scope='usa',
                projection=dict( type='albers usa' ),
                showland = True,
                landcolor = 'rgb(217, 217, 217)',
                subunitwidth=1,
                countrywidth=1,
                subunitcolor="rgb(255, 255, 255)",
                countrycolor="rgb(255, 255, 255)"
            )
        )

    fig = dict(data=airports, layout=layout)
    py.offline.iplot(fig)


# In[25]:

# Plots <Adjust> scale (Number at the end and title)

apPlot(originFlight, "Number of flights", 13.0*12)
apPlot(cancelledFlight, "Cancelled flights", 0.2*12)
apPlot(delayedFlight, "Delayed flights", 3.0*12)


# In[26]:

#TO-DO, hypothesize why are there delays, such as route popularity
#(Delay, ORIGIN-DEST_normalizeflight) and airport popularity (Delay, flightsPerAirport_normalizeflight)


# In[27]:

#Scatter plot for airport departure delay frequency vs number of flights

# Take top x airports <Adjust>

top = 100

# Scientific libraries
from numpy import arange,array,ones,asarray
from scipy import stats

share=delayedFlightShare.map(lambda a:a[1][0]*100).cache()
flights=delayedFlightShare.map(lambda a:float(a[1][1])/1000).cache()

#delayFlightExclude=delayedFlightShareExclude.map(lambda a:a[1][0]*100).cache()
#flightsExclude=delayedFlightShareExclude.map(lambda a:float(a[1][1])/1000).cache()


x = asarray(flights.collect()[:top])
y = asarray(share.collect()[:top])

xFull = asarray(flights.collect()[top+1:])
yFull = asarray(share.collect()[top+1:])

# Generated linear fit
slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)
line = slope*x+intercept

#Generate R

R = stats.pearsonr(x,y)

# Creating the dataset, and generating the plot
trace1 = go.Scatter(
    x=x,
    y=y,
    mode='markers',
    marker=go.Marker(color='rgb(255, 127, 14)'),
    name='Top 100 Airports',
    text = delayedFlightShare.map(lambda a:"Airport: " + a[0]).collect()[:top],
)

trace2 = go.Scatter(
    x=x,
    y=line,
    mode='lines',
    marker=go.Marker(color='rgb(31, 119, 180)'),
    name='Best Fit Line (Top 100)',
)

trace3 = go.Scatter(
    x=xFull,
    y=yFull,
    mode='markers',
    marker=go.Marker(color='lightgrey'),
    name='Other Airports',
    text = delayedFlightShare.map(lambda a:"Airport: " + a[0]).collect()[top+1:],
)

annotation = go.Annotation(
    x=3.5,
    y=1,
    text='',
    showarrow=False,
    font=go.Font(size=16)
)
layout = go.Layout(
    title='Precentage Delay vs Flights Per Year',
    plot_bgcolor='rgb(229, 229, 229)',
    xaxis=go.XAxis(zerolinecolor='rgb(255,255,255)', gridcolor='rgb(255,255,255)', title = "Flights per year (Thousands)"),
    yaxis=go.YAxis(zerolinecolor='rgb(255,255,255)', gridcolor='rgb(255,255,255)', title = "Percentage flight delayed"),
    annotations=[annotation]
)

data = [trace3, trace1, trace2]
fig = go.Figure(data=data, layout=layout)

py.offline.iplot(fig)

print ("Slope is: %s" % slope)
print ("R is: %f" % R[0])


# In[28]:

#Scatter plot for airport departure delay frequency vs number of flights

# Take top x airports <Adjust>

share=averageDelayByAirport.map(lambda a:a[1][0]).cache()
flights=averageDelayByAirport.map(lambda a:float(a[1][1])/1000).cache()

#delayFlightExclude=delayedFlightShareExclude.map(lambda a:a[1][0]*100).cache()
#flightsExclude=delayedFlightShareExclude.map(lambda a:float(a[1][1])/1000).cache()


x = asarray(flights.collect()[:top])
y = asarray(share.collect()[:top])

xFull = asarray(flights.collect()[top+1:])
yFull = asarray(share.collect()[top+1:])

# Generated linear fit
slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)
line = slope*x+intercept

#Generate R

R = stats.pearsonr(x,y)

# Creating the dataset, and generating the plot
trace1 = go.Scatter(
    x=x,
    y=y,
    mode='markers',
    marker=go.Marker(color='rgb(255, 127, 14)'),
    name='Top 100 Airports',
    text = averageDelayByAirport.map(lambda a:"Airport: " + a[0]).collect()[:top],
)

trace2 = go.Scatter(
    x=x,
    y=line,
    mode='lines',
    marker=go.Marker(color='rgb(31, 119, 180)'),
    name='Best Fit Line (Top 100)',
)

trace3 = go.Scatter(
    x=xFull,
    y=yFull,
    mode='markers',
    marker=go.Marker(color='lightgrey'),
    name='Other Airports',
    text = averageDelayByAirport.map(lambda a:"Airport: " + a[0]).collect()[top+1:],
)

annotation = go.Annotation(
    x=3.5,
    y=1,
    text='',
    showarrow=False,
    font=go.Font(size=16)
)
layout = go.Layout(
    title='Average Delay vs Flights Per Year',
    plot_bgcolor='rgb(229, 229, 229)',
    xaxis=go.XAxis(zerolinecolor='rgb(255,255,255)', gridcolor='rgb(255,255,255)', title = "Flights per year (Thousands)"),
    yaxis=go.YAxis(zerolinecolor='rgb(255,255,255)', gridcolor='rgb(255,255,255)', title = "Average delay"),
    annotations=[annotation]
)

data = [trace3, trace1, trace2]
fig = go.Figure(data=data, layout=layout)

py.offline.iplot(fig)

print ("Slope is: %s" % slope)
print ("R is: %f" % R[0])


# In[ ]:




# In[30]:

smallRoute = smallRDD.map(lambda x:(str(x.DayofMonth)+"/"+str(x.Month)+"/"+str(x.Year), x.DayOfWeek, x.Origin+"-"+x.Dest, x.DepDelayMinutes, x.ArrDelayMinutes, x.TailNum, x.CarrierDelay, x.WeatherDelay, x.NASDelay, x.SecurityDelay, x.LateAircraftDelay))
print smallRoute.take(10)

#Observe the data of routes
routeOccurNum = smallRoute.map(lambda x:(x[2], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda (x,y):-y)
topTwentyFrequentRoute = routeOccurNum.take(20)
#display(topTwentyFrequentRoute, "Route", "Frequency")

#Find out the delay on air
nonullSmallRoutemid = smallRoute.filter(lambda x: (x[3])!=None)
nonullSmallRoute = nonullSmallRoutemid.filter(lambda x: (x[4])!=None)
nonullSmallRoute = nonullSmallRoute.filter(lambda x: x)
airSmallRoute = nonullSmallRoute.map(lambda x: (x[2], int(x[4]-x[3]) if x[4] > x[3] else 0))
#display(airSmallRoute.take(10))

#the percentage delay and average delay time (include no delay) is calculated on air
percentageAirDelay = airSmallRoute.map(lambda x:(x[0], [1 if x[1] > 0 else 0, 1])).reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]]).map(lambda x: [x[0], 100* x[1][0] / x[1][1]]).sortBy(lambda (x,y):-y)
averageAirDelay = airSmallRoute.map(lambda x:(x[0], [x[1] if x[1] > 0 else 0,1])).reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]]).map(lambda x: [x[0], 1.0* x[1][0] / x[1][1]]).sortBy(lambda (x,y):-y)
airDelayFull = routeOccurNum.join(percentageAirDelay).sortBy(lambda (x,(y,z)):-y)


# In[32]:

print airDelayFull.take(2)


# In[40]:

#Scatter plot for airport departure delay frequency vs number of flights

# Take top x airports <Adjust>
top = 500

share=airDelayFull.map(lambda a:a[1][1]).cache()
flights=airDelayFull.map(lambda a:float(a[1][0])/1000).cache()

#delayFlightExclude=delayedFlightShareExclude.map(lambda a:a[1][0]*100).cache()
#flightsExclude=delayedFlightShareExclude.map(lambda a:float(a[1][1])/1000).cache()


x = asarray(flights.collect()[:top])
y = asarray(share.collect()[:top])

xFull = asarray(flights.collect()[top+1:])
yFull = asarray(share.collect()[top+1:])

# Generated linear fit
slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)
line = slope*x+intercept

#Generate R

R = stats.pearsonr(x,y)

# Creating the dataset, and generating the plot
trace1 = go.Scatter(
    x=x,
    y=y,
    mode='markers',
    marker=go.Marker(color='rgb(255, 127, 14)'),
    name='Top 500 Routes',
    text = airDelayFull.map(lambda a:"Route: " + a[0]).collect()[:top],
)

trace2 = go.Scatter(
    x=x,
    y=line,
    mode='lines',
    marker=go.Marker(color='rgb(31, 119, 180)'),
    name='Best Fit Line (Top 500)',
)

trace3 = go.Scatter(
    x=xFull,
    y=yFull,
    mode='markers',
    marker=go.Marker(color='lightgrey'),
    name='Other Routes',
    text = airDelayFull.map(lambda a:"Airport: " + a[0]).collect()[top+1:],
)

annotation = go.Annotation(
    x=3.5,
    y=1,
    text='',
    showarrow=False,
    font=go.Font(size=16)
)
layout = go.Layout(
    title='Average Delay vs Flights Per Route',
    plot_bgcolor='rgb(229, 229, 229)',
    xaxis=go.XAxis(zerolinecolor='rgb(255,255,255)', gridcolor='rgb(255,255,255)', title = "Flights Per Route (Thousands)"),
    yaxis=go.YAxis(zerolinecolor='rgb(255,255,255)', gridcolor='rgb(255,255,255)', title = "Average delay"),
    annotations=[annotation]
)

data = [trace3, trace1, trace2]
fig = go.Figure(data=data, layout=layout)

py.offline.iplot(fig)

print ("Slope is: %s" % slope)
print ("R is: %f" % R[0])


# In[36]:

#NAS Delay Investigation
#Then, the percentage delay and average delay time (include no delay) is calculated for arrival
percentageNasDelay = smallRoute.map(lambda x:(x[2], [1 if x[8] > 0 else 0, 1])).reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]]).map(lambda x: [x[0], 100* x[1][0] / x[1][1]]).sortBy(lambda (x,y):-y)
NASDelaySize = routeOccurNum.join(percentageNasDelay).sortBy(lambda (x,(y,z)):-y)


# In[37]:

#Scatter plot for airport departure delay frequency vs number of flights

# Take top x airports <Adjust>

share=NASDelaySize.map(lambda a:a[1][1]).cache()
flights=NASDelaySize.map(lambda a:float(a[1][0])/1000).cache()

#delayFlightExclude=delayedFlightShareExclude.map(lambda a:a[1][0]*100).cache()
#flightsExclude=delayedFlightShareExclude.map(lambda a:float(a[1][1])/1000).cache()


x = asarray(flights.collect()[:top])
y = asarray(share.collect()[:top])

xFull = asarray(flights.collect()[top+1:])
yFull = asarray(share.collect()[top+1:])

# Generated linear fit
slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)
line = slope*x+intercept

#Generate R

R = stats.pearsonr(x,y)

# Creating the dataset, and generating the plot
trace1 = go.Scatter(
    x=x,
    y=y,
    mode='markers',
    marker=go.Marker(color='rgb(255, 127, 14)'),
    name='Top 100 Airports',
    text = NASDelaySize.map(lambda a:"Airport: " + a[0]).collect()[:top],
)

trace2 = go.Scatter(
    x=x,
    y=line,
    mode='lines',
    marker=go.Marker(color='rgb(31, 119, 180)'),
    name='Best Fit Line (Top 100)',
)

trace3 = go.Scatter(
    x=xFull,
    y=yFull,
    mode='markers',
    marker=go.Marker(color='lightgrey'),
    name='Other Airports',
    text = NASDelaySize.map(lambda a:"Airport: " + a[0]).collect()[top+1:],
)

annotation = go.Annotation(
    x=3.5,
    y=1,
    text='',
    showarrow=False,
    font=go.Font(size=16)
)
layout = go.Layout(
    title='Average Delay vs Flights Per Year',
    plot_bgcolor='rgb(229, 229, 229)',
    xaxis=go.XAxis(zerolinecolor='rgb(255,255,255)', gridcolor='rgb(255,255,255)', title = "Flights per year (Thousands)"),
    yaxis=go.YAxis(zerolinecolor='rgb(255,255,255)', gridcolor='rgb(255,255,255)', title = "Average delay"),
    annotations=[annotation]
)

data = [trace3, trace1, trace2]
fig = go.Figure(data=data, layout=layout)

py.offline.iplot(fig)

print ("Slope is: %s" % slope)
print ("R is: %f" % R[0])


# In[ ]:



