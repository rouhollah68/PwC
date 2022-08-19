# importing libraries needed
import os
import glob
import pandas as pd
import difflib
import requests as rq
from datetime import datetime as dt
import datetime
import haversine as hs
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.offline as py
import plotly.graph_objects as go


# define directory
path='C:/Users/rouho/Desktop/pwc/data/'
os.chdir(path)

# Getting ducking stations cordinates
locations = pd.read_json(rq.get('https://api.tfl.gov.uk/BikePoint').text)
locations_start=locations[['commonName', 'lat', 'lon']].copy()
locations_start.columns=['StartStation Name','lat start','lon start']
locations_end=locations[['commonName', 'lat', 'lon']].copy()
locations_end.columns=['EndStation Name','lat end','lon end']
locations=locations[['commonName','lat', 'lon']]
locations.columns=['location','lat', 'lon']

# creating date range to orgenize raw data
date_range = pd.date_range(start='12/1/2020', end='7/31/2022' , freq='1M')

#listing csv files
files=glob.glob('*202*.csv')

# spreating monthly data
full_data={}

for i in date_range:
    full_data[str(i).split(' ')[0]]=pd.DataFrame()

for j in files:
    df=pd.read_csv(j,parse_dates=['End Date','Start Date'])
    for i in range(len(date_range)-1):
        df1=df[(df['Start Date']>=str(date_range[i]).split(' ')[0])& (df['Start Date']<str(date_range[i+1]).split(' ')[0])]
        if len(df1)>1:
            full_data[str(date_range[i]).split(' ')[0]]=pd.concat([full_data[str(date_range[i]).split(' ')[0]],df1])
    print(j)

for i in list(full_data.keys()):
    if len(full_data[i])==0:
        full_data.pop(i)

#moving to next directory to save monthly data

os.chdir('monthly/')

#cleaing and adding new variable to monthly data and saving them
for i in full_data.keys():
    df=full_data[i]
    df=pd.merge(df,locations_end, on='EndStation Name',how='outer')
    df=pd.merge(df,locations_start,on='StartStation Name',how='outer')
    df.dropna(inplace=True)
    df=df.sort_values(['Start Date'])
    df['Day_of_week']=[dt.weekday(z) for z in df['Start Date']]
    df['is_weekend']=df['Day_of_week']>=5
    df=df[['Rental Id','Duration','End Date','Start Date','lat end', 'lon end', 'lat start', 'lon start','Day_of_week','is_weekend','EndStation Name','StartStation Name']]
    #convert duration from secong to minute
    df['Duration']=df['Duration']/60
    df.reset_index(inplace=True,drop=True)
    df.to_csv(i+'.csv',index=False)

# listing clean monthly data
#os.chdir('monthly/')
monthly_data=glob.glob('*.csv')

# Importing data set and concat all to one DataFrame
df = []
for filename in monthly_data:
    df.append(pd.read_csv(filename,parse_dates=['Start Date','End Date']))

df = pd.concat(df, ignore_index=True)

# Calculating each trip distance in mile

distance=[]

for i in  range(len(df)):
    loc1=(df['lat start'][i],df['lon start'][i])
    loc2=(df['lat end'][i],df['lon end'][i])
    distance.append(hs.haversine(loc1,loc2,unit=hs.Unit.MILES))

df['distance_in_mile']=distance
number_of_stations=len(df['StartStation Name'].unique())
# Average and sum of Duration, number of bike rental, and mile travled
df1=df[['Rental Id','Duration', 'Start Date','distance_in_mile']]
df1.index=df1['Start Date']
daily_data_sum=df1.resample('1D').agg({'Rental Id': 'count', 'Duration': 'sum','distance_in_mile':'sum'}).rename(columns={'Rental Id':'Number_of_bike'})
average_daily_rental=daily_data_sum.mean()[0]

h4_data_mean=df1.resample('4H').agg({'Rental Id': 'count', 'Duration': 'mean','distance_in_mile':'mean'}).rename(columns={'Rental Id':'Number_of_bike'})
h4_data_mean['time_of_day']=[i.hour for i in h4_data_mean.index]
h4_summary=pd.DataFrame([h4_data_mean[h4_data_mean['time_of_day']==i].mean()[:3] for i in [0,4,8,12,16,20]])
h4_summary.index=['00:00 - 04:00','04:00 - 08:00','08:00 - 12:00','12:00 - 16:00','16:00 - 20:00','20:00 - 24:00']
os.chdir('result/')
h4_summary.to_csv('h4_summary.csv')

#ploting function
def plot(df,name):
    fig = make_subplots(rows=1, cols=3, shared_yaxes=False)
    duration_plot= go.Scatter(x=df.index, y=df["Duration"], name="Duration")
    number_of_bike_plot= go.Scatter(x=df.index, y=df["Number_of_bike"], name="Number of Bike")
    distance_in_mile_plot= go.Scatter(x=df.index, y=df["distance_in_mile"], name="Distance in Mile")
    fig.add_trace(duration_plot, row=1, col=1)
    fig.add_trace(number_of_bike_plot, row=1, col=2)
    fig.add_trace(distance_in_mile_plot, row=1, col=3)
    fig.write_image(name+'.png')
plot(h4_summary,'every 4 hours summary')


# Most frequant pick up, drop off, route
top_20_pickup=pd.DataFrame(df['StartStation Name'].value_counts()[:20]).reset_index()
top_20_pickup.columns=['location','Number_of_bike']
pd.merge(top_20_pickup,locations,how='left').to_csv('top_20_pickup.csv')

top_20_dropoff=pd.DataFrame(df['EndStation Name'].value_counts()[:20]).reset_index()
top_20_dropoff.columns=['location','Number_of_bike']
pd.merge(top_20_dropoff,locations,how='left').to_csv('top_20_dropoff.csv')

df['route']=df['StartStation Name']+' / '+df['EndStation Name']
top_20_routes=df['route'].value_counts()[:20]
start=[i.split(' / ')[0] for i in top_20_routes.index]
end=[i.split(' / ')[1] for i in top_20_routes.index]
top_20_routes=pd.DataFrame([start,end,top_20_routes.values]).T.rename(columns={0:'Start',1:'End',2:'Number of Trip'})
top_20_routes.to_csv('top_20_routes.csv')

returend_to_same_location=top_20_routes[top_20_routes['Start']==top_20_routes['End']]

#  Finding the most travled return trip
unique_route=list(set(df['StartStation Name']+' / '+df['EndStation Name']))
reversed_unique_route=[i.split(' / ')[1]+' / '+i.split(' / ')[0] for i in unique_route]
df_routes=pd.DataFrame([unique_route,reversed_unique_route]).T
df_routes.columns=['route','reversed_route']
df_routes['route_number']=range(1,len(df_routes)+1)
df_routes['reversed_routes_number']=df_routes['route_number']*-1
df=pd.merge(df,df_routes[['route','route_number']],how='left',left_on='route', right_on='route')
df=pd.merge(df,df_routes[['reversed_route','reversed_routes_number']],left_on='route', right_on='reversed_route')
one_way=pd.DataFrame(df['route_number'].value_counts()).reset_index()
one_way.columns=['route_number','number_of_travels_1']
return_trip=pd.DataFrame(df['reversed_routes_number'].value_counts()).reset_index()
return_trip.columns=['route_number','number_of_travels_2']
return_trip['route_number']=return_trip['route_number']*-1
round_trip=pd.merge(one_way,return_trip,how='outer')
round_trip['total_trip']=round_trip['number_of_travels_1']+round_trip['number_of_travels_2']
round_trip=pd.merge(round_trip,df_routes[['route','route_number']],how='left')[['route','total_trip']]

start=[i.split(' / ')[0] for i in round_trip.route]
end=[i.split(' / ')[1] for i in round_trip.route]
round_trip=pd.DataFrame([start,end,round_trip.total_trip]).T.rename(columns={0:'Start',1:'End',3:'Number of trip'}).head(20)
round_trip=round_trip.head(20)
round_trip.to_csv('round_trip.csv')
round_trip[round_trip['Start']==round_trip['End']]

# average daily and 4 hours dem,and
df_demand=df[['Start Date','StartStation Name']]
df_demand.index=df_demand['Start Date']
df_demand_daily=df_demand.groupby([pd.Grouper(freq='d'), 'StartStation Name']).agg(['count']).unstack().replace(np.nan, 0)
df_demand_daily.columns = [col[2] for col in df_demand_daily.columns]
df_demand_4h=df_demand.groupby([pd.Grouper(freq='4h'), 'StartStation Name']).agg(['count']).unstack().replace(np.nan, 0)
df_demand_4h.columns = [col[2] for col in df_demand_4h.columns]

# average daily and 4 hours supply
df_supply=df[['Start Date','EndStation Name']]
df_supply.index=df_supply['Start Date']
df_supply_daily=df_supply.groupby([pd.Grouper(freq='d'), 'EndStation Name']).agg(['count']).unstack().replace(np.nan, 0)
df_supply_daily.columns = [col[2] for col in df_supply_daily.columns]
df_supply_4h=df_supply.groupby([pd.Grouper(freq='4h'), 'EndStation Name']).agg(['count']).unstack().replace(np.nan, 0)
df_supply_4h.columns = [col[2] for col in df_supply_4h.columns]

# comparing the average demand of 0:00-4:00, 4:00-8:00, 8:00-12:00, 12:00-16:00, 16:00-20:00, and 20:00-24:00
df_demand_4h['time_of_day']=[i.hour for i in df_demand_4h.index]

# comparing the average supply of 0:00-4:00, 4:00-8:00, 8:00-12:00, 12:00-16:00, 16:00-20:00, and 20:00-24:00
df_supply_4h['time_of_day']=[i.hour for i in df_supply_4h.index]

# the diffrence between demand and supply shows the flow of the bike
difference_pickup_dropoff=df_demand_4h-df_supply_4h
difference_pickup_dropoff['time_of_day']=[i.hour for i in difference_pickup_dropoff.index]
summary=pd.melt(difference_pickup_dropoff.groupby(['time_of_day']).mean())
summary['time_of_day']=[0,4,8,12,16,20]*int(len(summary)/6)
summary.columns=['location','mean','time_of_day']


H0=pd.merge(summary[summary.time_of_day==0].sort_values('mean',ascending=False),locations,how='left').drop(['time_of_day'],axis=1)
H4=pd.merge(summary[summary.time_of_day==4].sort_values('mean',ascending=False),locations,how='left').drop(['time_of_day'],axis=1)
H8=pd.merge(summary[summary.time_of_day==8].sort_values('mean',ascending=False),locations,how='left').drop(['time_of_day'],axis=1)
H12=pd.merge(summary[summary.time_of_day==12].sort_values('mean',ascending=False),locations,how='left').drop(['time_of_day'],axis=1)
H16=pd.merge(summary[summary.time_of_day==16].sort_values('mean',ascending=False),locations,how='left').drop(['time_of_day'],axis=1)
H20=pd.merge(summary[summary.time_of_day==20].sort_values('mean',ascending=False),locations,how='left').drop(['time_of_day'],axis=1)

H0=pd.concat([H0.head(10),H0.tail(10)]).reset_index(drop=True)
H4=pd.concat([H4.head(10),H4.tail(10)]).reset_index(drop=True)
H8=pd.concat([H8.head(10),H8.tail(10)]).reset_index(drop=True)
H12=pd.concat([H12.head(10),H12.tail(10)]).reset_index(drop=True)
H16=pd.concat([H16.head(10),H16.tail(10)]).reset_index(drop=True)
H20=pd.concat([H20.head(10),H20.tail(10)]).reset_index(drop=True)

pd.concat([H0.location,H4.location,H8.location,H12.location,H16.location,H20.location],ignore_index=True,axis=1).rename(columns={0:'00:00 - 04:00',1:'04:00 - 08:00',2:'08:00 - 12:00',3:'12:00 - 16:00',4:'16:00 - 20:00',5:'20:00 - 24:00'}).to_csv('S-D.csv',index=False)
