#%%

#Import packages
import pandas as pd
from datetime import timedelta

# %%

# Read both csv files
TMY3_StationMeta = pd.read_csv('TMY3_StationsMeta.csv')
tmy3 = pd.read_csv('tmy3.csv')
#%%

# Extracting the relevant features from tmy3
tmy3_extract = tmy3[['Date (MM/DD/YYYY)', 'Time (HH:MM)','GHI (W/m^2)', 'DNI (W/m^2)', 'station_number']]

#%%
# Concat Date and Time into one feature
tmy3_extract["DateTime"] = tmy3_extract["Date (MM/DD/YYYY)"].astype(str) + " " + tmy3_extract["Time (HH:MM)"].astype(str)

#%%
# Convert to datetime by fixing the 24:00 into 00:00 of the next day
tmy3_extract["DateTimeFixed"] = tmy3_extract['DateTime'].str.replace('24:', '00:', regex=True) # First fix the 24:00 into 00:00 of the next day
tmy3_extract["DateTimeFixed"] = pd.to_datetime(tmy3_extract["DateTimeFixed"], format='%m/%d/%Y %H:%M', utc=True) # Convert to datetime format
tmy3_extract.loc[tmy3_extract["Time (HH:MM)"] == "24:00","DateTimeFixed"] = tmy3_extract["DateTimeFixed"] + pd.Timedelta(days=1) # Make it the next day

#%%
# Extracting the relevant features from TMY3_StationMeta
StationMeta_extract = TMY3_StationMeta.iloc[:,0:5].rename(columns={'USAF': 'id', 'Site Name': 'site_name'})

# Extracting the relevant features from tmy3_extract
tmy3_extract.rename(columns={'station_number': 'id'}, inplace=True)
TMY3_df = tmy3_extract[['id', 'DateTimeFixed', 'GHI (W/m^2)', 'DNI (W/m^2)']].set_index('DateTimeFixed').copy()

#%%

count = 0
TMY3_weekly = pd.DataFrame(columns=['id','site_name','coordinates','data'])#.set_index('id')

for station in TMY3_df['id'].unique(): #1020 stations
    #if station == 690150: #(For testing only)        
        
        # Retrieve site name
        site_name = StationMeta_extract[StationMeta_extract['id'] == station].iloc[0,0]
        
        # Generate coordinates for each site
        coordinates_df = StationMeta_extract[StationMeta_extract['id'] == station].iloc[:,3:5]
                
        # Resample data to weekly then generate timestamp (MS) since epoch
        resample_df = TMY3_df[TMY3_df['id'] == station].iloc[:,1:].resample('W').mean()
        resample_df.insert(0, 'timestamp', resample_df.index.to_series().apply(lambda x: x.timestamp()))
        resample_df.reset_index(drop=True) # Dropping datetime index #(optional)

        # Package each site's coordinates and date into TMY3_weekly         
        TMY3_weekly.loc[count] = [station, site_name, coordinates_df, resample_df]
        count += 1

TMY3_weekly.to_json('Weekly_Weather_Data_by_Station.json')


# %%

# To check nested dataframe: print(TMY3_weekly['data'].iloc[0])

# Graveyard
"""
# Concat Long and Lat into one feature
df = pd.DataFrame({'ID': StationMeta_extract['station_number'], 'Name': StationMeta_extract['Site Name'],'Coordinates': pd.Series({'Latitude': StationMeta_extract["Latitude"],
                            'Longitude': StationMeta_extract["Longitude"]})})
OR
coordinates = pd.DataFrame({'Latitude': StationMeta_extract["Latitude"],
                            'Longitude': StationMeta_extract["Longitude"]})
StationMeta_extract['coordinates'] = coordinates
StationMeta_df = pd.DataFrame({'station_number':[1,2,3], 'coordinates':[coordinates]})

"""
"""
# Joining both dataset into a one dataframe
TMY3 = pd.merge(tmy3_extract, StationMeta_extract, on = "id", how = "outer") 
"""
