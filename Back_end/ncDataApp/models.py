from django.db import models
import warnings
warnings.filterwarnings('ignore')
import numpy as np
from geojson import Point, Feature, FeatureCollection
import xarray as xr
import pandas as pd
import time
import json
import glob
import datetime

# Create your models here.

class NcData(models.Model):
    def nc_read_fun(self,request):
        ds = xr.open_dataset('Nc_data/cesm2cam6v2.2018-01-01.00.clm2.h0.2018-01-01-00000.nc', engine="netcdf4")

        # Conversion of request from django.core.handlers.wsgi.WSGIRequest to dict data type
        body_unicode = request.body.decode('utf-8')
        body = json.loads(body_unicode)
        print(body['lat'])
        lat = np.array(body['lat'])
        lon = np.array(body['lon'])
        soilLevel = np.array(body['soilLevel'])
        dayCount = np.array(body['DayCount'])

        # To set the soil level value
        if(soilLevel != ""):
            j = int(soilLevel)
        else:
            j = 0
        
        # To set the Day Count
        if(dayCount != ""):
            dCount = int(dayCount)
        else:
            dCount = 10

        # Longitude Conversion
        lon = ((lon+180)%360)-180

        # Converting into timestamp
        datetimeindex = ds.indexes['time'].to_datetimeindex()
        ds['time'] = datetimeindex
        date_df = pd.to_datetime(ds['time'])

        features=[]
        # Count of DZSOI and ZSOI
        count = len(ds.ZSOI)
        SoilLevl_count = len(ds.levsoi)

        coords = (float(lon),float(lat))
        H2OSOI_ds = ds.H2OSOI.sel(lat=lat, lon=lon, method='nearest').values
        for a in range(0,dCount):
            features.append(Feature(geometry = Point(coords),properties={"Time":int(time.mktime(date_df[a].timetuple()))*1000,"Soil Level":int(ds.levsoi[j].values),"H2OSOI":float(H2OSOI_ds[a][j])}))
            feature_collection = FeatureCollection(features)
        Json_dump = json.dumps(feature_collection)
        return Json_dump

class neonData(models.Model):
    def neon_read_fun(self,request):

        # Conversion of request from django.core.handlers.wsgi.WSGIRequest to dict data type
        body_unicode = request.body.decode('utf-8')
        body = json.loads(body_unicode)
        print(body['lat'])
        lat = np.array(body['lat'])
        lon = np.array(body['lon'])
        site = np.array(body['SiteName'])
        
        ds = xr.open_dataset('Nc_data/cesm2cam6v2.2018-01-01.00.clm2.h0.2018-01-01-00000.nc', engine="netcdf4")

        #H2OSOI variable (location of 5 Site)
        if site == 'TALL':
            loc_1_H2OSOI=ds.H2OSOI.sel(lat=32.950, lon=272.607, method='nearest')
        elif site == 'ABBY':
            loc_1_H2OSOI=ds.H2OSOI.sel(lat=45.762, lon=237.67, method='nearest')
        elif site == 'BLAN':
            loc_1_H2OSOI=ds.H2OSOI.sel(lat=39.060, lon=281.928, method='nearest')
        elif site == 'CLBJ':
            loc_1_H2OSOI=ds.H2OSOI.sel(lat=33.401, lon=262.430, method='nearest')
        elif site == 'WOOD':
            loc_1_H2OSOI=ds.H2OSOI.sel(lat=47.128, lon=260.759, method='nearest')
        else:
            loc_1_H2OSOI=ds.H2OSOI.sel(lat=32.950, lon=272.607, method='nearest')
        loc_1_H2OSOI
        data_H2OSOI = pd.DataFrame(loc_1_H2OSOI)
        data_H2OSOI_5 = data_H2OSOI.iloc[ :31 , : 5]
        
        
        #Finding the mean value for the first five values
        data_H2OSOI_5_mean = data_H2OSOI_5.mean(axis = 1, skipna = True)
        data_H2OSOI_5_mean_table = pd.concat([data_H2OSOI_5_mean],axis = 1, join = "inner")
        data_H2OSOI_5_mean_table.columns = ['NCAR_H2OSOI_Mean']
        data_H2OSOI_5_mean_table

        # Creating a collection of data frame to store various sites in diffrent frames
        dataframe_collection = {}

        # Creating a temporary data frame to store temp values for processing
        all_data = pd.DataFrame() 

        #Setting the date range using pandas
        klist= pd.date_range('2018-01','2018-02',freq='M').strftime("%Y-%m").tolist()
        if(site == "ABBY"):
            site_name = "Abby_site"
            site_num = "D16.ABBY"
        elif (site == "BLAN"):
            site_name = "Blan_site"
            site_num = "D02.BLAN"
        elif (site == "CLBJ"):
            site_name = "CLBJ_site"
            site_num = "D11.CLBJ"
        elif (site == "WOOD"):
            site_name = "Wood_site"
            site_num = "D09.WOOD"
        elif (site == "TALL"):
            site_name = "Tall_site"
            site_num = "D08.TALL"
        else:
            site_name = "Default"
            site_num = "Default"
        #using 3 Loops, k loop to manage the date range (change if required), j loop is to manage the sites since (we chose 5 sites ranges from 1-6), the third loop is to iterate through diffrent files inside a given directory
        for k in range(0,1):
            for j in range(1,6):
                i=1
                for f in glob.glob("Neon_data/%s/NEON_conc-h2o-soil-salinity/NEON.%s.DP1.00094.001.%s.expanded.20220120T173946Z.RELEASE-2022/NEON.%s.DP1.00094.001.00%d.*.030.*"%(site_name,site_num,klist[k],site_num,j)):
        #reading a single file and deleting and formatting the data based on our requirement
                    df = pd.read_csv(f)
                    del df['VSWCMinimum']
                    del df['VSWCMaximum']
                    del df['VSWCNumPts']
                    del df['VSWCExpUncert']
                    del df['VSWCStdErMean']
                    del df['VSICMinimum']
                    del df['VSICMaximum']
                    del df['VSICNumPts']
                    del df['VSICExpUncert']
                    del df['VSICStdErMean']
                    del df['VSWCVariance']
                    del df['VSICVariance']
                    df['startDateTime']= pd.to_datetime(df['startDateTime'])

        #resampling our existing data by averaging our data to daily data index is start date time 
                    df=df.resample('d', on='startDateTime').mean()
        #Checking whether this is the first file for a given site which is being processes if first add to the orginal data frame since it is empty      
                    if k==0:
                        if i==1:
                            dataframe_collection[j] = pd.DataFrame(index = df.index)
                            dataframe_collection[j]['Level1VSWCMean'] = df['VSWCMean']
                            dataframe_collection[j]['Level1VSWCFinalQF'] = df['VSWCFinalQF']
                            dataframe_collection[j]['Level1VSICMean'] = df['VSICMean']
                            dataframe_collection[j]['Level1VSICFinalQF'] = df['VSICFinalQF'] 

                        else:
                            dataframe_collection[j]['Level%dVSWCMean'%i] = df['VSWCMean']
                            dataframe_collection[j]['Level%dVSWCFinalQF' %i] = df['VSWCFinalQF']
                            dataframe_collection[j]['Level%dVSICMean' %i] = df['VSICMean']
                            dataframe_collection[j]['Level%dVSICFinalQF'%i] = df['VSICFinalQF']
        #if it is not the first file for a given site then we store the processed data in a temporary data frame and append it after processing
                    else:
                        if i==1:
                            all_data = pd.DataFrame(index = df.index)
                            all_data['Level1VSWCMean'] = df['VSWCMean']
                            all_data['Level1VSWCFinalQF'] = df['VSWCFinalQF']
                            all_data['Level1VSICMean'] = df['VSICMean']
                            all_data['Level1VSICFinalQF'] = df['VSICFinalQF'] 

                        else:
                            all_data['Level%dVSWCMean'%i] = df['VSWCMean']
                            all_data['Level%dVSWCFinalQF' %i] = df['VSWCFinalQF']
                            all_data['Level%dVSICMean' %i] = df['VSICMean']
                            all_data['Level%dVSICFinalQF'%i] = df['VSICFinalQF']
                    i=i+1
        #appending the data frame
                    dataframe_collection[j]=dataframe_collection[j].append(all_data)
        #stripping our timezone from datetime so that we can get it into a format which can be stored in excel)
        dataframe_collection[1].index=dataframe_collection[1].index.astype(str).str[:-6]
        dataframe_collection[2].index=dataframe_collection[2].index.astype(str).str[:-6]
        dataframe_collection[3].index=dataframe_collection[3].index.astype(str).str[:-6]
        dataframe_collection[4].index=dataframe_collection[4].index.astype(str).str[:-6]
        dataframe_collection[5].index=dataframe_collection[5].index.astype(str).str[:-6]

        #writing the cleaned data to a new excel file where 5 sites are stored in 5 diffrent sheets        
        fname = 'Neon_data/'+site_name+'/Final_Data - 2018.xlsx'
        writer = pd.ExcelWriter(fname,engine = 'xlsxwriter',options = {'remove_timezone': True})
        dataframe_collection[1].to_excel(writer, "Site1")
        dataframe_collection[2].to_excel(writer, "Site2")
        dataframe_collection[3].to_excel(writer, "Site3")
        dataframe_collection[4].to_excel(writer, "Site4")
        dataframe_collection[5].to_excel(writer, "Site5")

        # more dataframes goes here
        writer.save()

        df_1 = pd.read_excel('Neon_data/'+site_name+'/Final_Data - 2018.xlsx', sheet_name='Site1')
        cols_1 = ['Level1VSWCMean', 'Level2VSWCMean', 'Level3VSWCMean', 'Level4VSWCMean', 'Level5VSWCMean', 'Level6VSWCMean', 'Level7VSWCMean', 'Level8VSWCMean']
        df_1['sum'] = df_1[cols_1].sum(axis=1)

        df_2 = pd.read_excel('Neon_data/'+site_name+'/Final_Data - 2018.xlsx', sheet_name='Site2')
        cols_2 = ['Level1VSWCMean', 'Level2VSWCMean', 'Level3VSWCMean', 'Level4VSWCMean', 'Level5VSWCMean', 'Level6VSWCMean', 'Level7VSWCMean', 'Level8VSWCMean']
        df_2['sum'] = df_2[cols_2].sum(axis=1)

        df_3 = pd.read_excel('Neon_data/'+site_name+'/Final_Data - 2018.xlsx', sheet_name='Site3')
        cols_3 = ['Level1VSWCMean', 'Level2VSWCMean', 'Level3VSWCMean', 'Level4VSWCMean', 'Level5VSWCMean', 'Level6VSWCMean', 'Level7VSWCMean', 'Level8VSWCMean']
        df_3['sum'] = df_3[cols_3].sum(axis=1)

        df_4 = pd.read_excel('Neon_data/'+site_name+'/Final_Data - 2018.xlsx', sheet_name='Site4')
        cols_4 = ['Level1VSWCMean', 'Level2VSWCMean', 'Level3VSWCMean', 'Level4VSWCMean', 'Level5VSWCMean', 'Level6VSWCMean', 'Level7VSWCMean', 'Level8VSWCMean']
        df_4['sum'] = df_4[cols_4].sum(axis=1)

        df_5 = pd.read_excel('Neon_data/'+site_name+'/Final_Data - 2018.xlsx', sheet_name='Site5')
        cols_5 = ['Level1VSWCMean', 'Level2VSWCMean', 'Level3VSWCMean', 'Level4VSWCMean', 'Level5VSWCMean', 'Level6VSWCMean', 'Level7VSWCMean', 'Level8VSWCMean']
        df_5['sum'] = df_5[cols_5].sum(axis=1)

        dff = df_1['sum'] + df_2['sum'] + df_3['sum'] + df_4['sum'] + df_5['sum']
        dff = dff/5

        df_date = []
        for i in range(len(df_5['startDateTime'])):
            df_date.append(df_5['startDateTime'][i][0:10])
        
        date_fram = pd.DataFrame(df_date)
        date_fram.columns = ["Date"]
        date_fram

        neon_data = pd.concat([dff], axis = 1, join = "inner")
        neon_data.columns = ['NEON_VSWC_Mean']
        neon_data
        data_combine = pd.concat([data_H2OSOI_5_mean_table, neon_data,date_fram], axis =1)
        json_val = data_combine.to_json()
        
        return json_val

class ReanalysisData(models.Model):
    def ReanalyseData_read_fun(self,request):
        # Conversion of request from django.core.handlers.wsgi.WSGIRequest to dict data type
        body_unicode = request.body.decode('utf-8')
        body = json.loads(body_unicode)
        print(body['lat'])
        lat = np.array(body['lat'])
        lon = np.array(body['lon'])
        site = np.array(body['SiteName'])
        YearCount = np.array(body['YearCount'])

        # To set the Year Count
        if(YearCount != ""):
            YCount = int(YearCount)
        else:
            YCount = 5
        # ERA5 Data formation
        ds_ERA5 = xr.open_dataset('ERA5/ERA5_SM_daily_1999_2021_0_5m.nc', engine="netcdf4")

        #swvRZ variable (location of 5 Site)
        if site == 'TALL':
            ERA5 =ds_ERA5.swvRZ.sel(lat=32.950, lon=272.607, method='nearest')
        elif site == 'ABBY':
            ERA5 =ds_ERA5.swvRZ.sel(lat=45.762, lon=237.67, method='nearest')
        elif site == 'BLAN':
            ERA5 =ds_ERA5.swvRZ.sel(lat=39.060, lon=281.928, method='nearest')
        elif site == 'CLBJ':
            ERA5 =ds_ERA5.swvRZ.sel(lat=33.401, lon=262.430, method='nearest')
        elif site == 'WOOD':
            ERA5 =ds_ERA5.swvRZ.sel(lat=47.128, lon=260.759, method='nearest')
        else:
            ERA5 =ds_ERA5.swvRZ.sel(lat=32.950, lon=272.607, method='nearest')

        ERA5_data_merge = xr.merge([ERA5], compat = 'override')
        ERA5_data_swvRZ = pd.DataFrame(ERA5_data_merge.swvRZ)
        ERA5_data_swvRZ.columns = ['ERA5']
        # Converting dataset to DataFrame
        #swvRZ_df = ERA5.to_dataframe()

        # MEERA_2 Data formation
        ds_MEERA_2 = xr.open_dataset("MEERA_2/MERRA2_SM_0_5m_1999_2018_organize.nc", engine="netcdf4")
        #RZMC variable (location of 5 Site)
        if site == 'TALL':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=32.950, lon=272.607, method='nearest')
        elif site == 'ABBY':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=45.762, lon=237.67, method='nearest')
        elif site == 'BLAN':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=39.060, lon=281.928, method='nearest')
        elif site == 'CLBJ':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=33.401, lon=262.430, method='nearest')
        elif site == 'WOOD':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=47.128, lon=260.759, method='nearest')
        else:
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=32.950, lon=272.607, method='nearest')

        MEERA_data_merge = xr.merge([MEERA_2], compat = 'override')
        MEERA_data_RZMC = pd.DataFrame(MEERA_data_merge.RZMC)
        MEERA_data_RZMC.columns = ['MEERA_2']

        # Combining ERA5 and MEERA_2
        combine_df = pd.concat([ERA5_data_swvRZ.iloc[:7305], MEERA_data_RZMC], axis = 1)
        # Day Count Filter
        if(YCount == 5):
            combine_df = combine_df[0:1826]
        elif(YCount == 10):
            combine_df = combine_df[0:3652]
        elif(YCount == 15):
            combine_df = combine_df[0:5478]
        elif(YCount == 20):
            combine_df = combine_df[0:7304]
        else:
            combine_df = combine_df[:]
        json_data = combine_df.to_json()

        return json_data

class MeanData_class(models.Model):
    def MeanData_read_fun(self,request):
        # Conversion of request from django.core.handlers.wsgi.WSGIRequest to dict data type
        body_unicode = request.body.decode('utf-8')
        body = json.loads(body_unicode)
        print(body['lat'])
        lat = np.array(body['lat'])
        lon = np.array(body['lon'])
        site = np.array(body['SiteName'])
        YearCount = np.array(body['YearCount'])

        # To set the Year Count
        if(YearCount != ""):
            YCount = int(YearCount)
        else:
            YCount = 5
        # ERA5 Data formation
        ds_ERA5 = xr.open_dataset('ERA5/ERA5_SM_daily_1999_2021_0_5m.nc', engine="netcdf4")

        #swvRZ variable (location of 5 Site)
        if site == 'TALL':
            ERA5 =ds_ERA5.swvRZ.sel(lat=32.950, lon=272.607, method='nearest')
        elif site == 'ABBY':
            ERA5 =ds_ERA5.swvRZ.sel(lat=45.762, lon=237.67, method='nearest')
        elif site == 'BLAN':
            ERA5 =ds_ERA5.swvRZ.sel(lat=39.060, lon=281.928, method='nearest')
        elif site == 'CLBJ':
            ERA5 =ds_ERA5.swvRZ.sel(lat=33.401, lon=262.430, method='nearest')
        elif site == 'WOOD':
            ERA5 =ds_ERA5.swvRZ.sel(lat=47.128, lon=260.759, method='nearest')
        else:
            ERA5 =ds_ERA5.swvRZ.sel(lat=32.950, lon=272.607, method='nearest')

        ERA5_data_merge = xr.merge([ERA5], compat = 'override')
        ERA5_data_swvRZ = pd.DataFrame(ERA5_data_merge.swvRZ)
        ERA5_data_swvRZ.columns = ['ERA5']
        # Converting dataset to DataFrame
        #swvRZ_df = ERA5.to_dataframe()

        # MEERA_2 Data formation
        ds_MEERA_2 = xr.open_dataset("MEERA_2/MERRA2_SM_0_5m_1999_2018_organize.nc", engine="netcdf4")
        #RZMC variable (location of 5 Site)
        if site == 'TALL':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=32.950, lon=272.607, method='nearest')
        elif site == 'ABBY':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=45.762, lon=237.67, method='nearest')
        elif site == 'BLAN':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=39.060, lon=281.928, method='nearest')
        elif site == 'CLBJ':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=33.401, lon=262.430, method='nearest')
        elif site == 'WOOD':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=47.128, lon=260.759, method='nearest')
        else:
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=32.950, lon=272.607, method='nearest')

        MEERA_data_merge = xr.merge([MEERA_2], compat = 'override')
        MEERA_data_RZMC = pd.DataFrame(MEERA_data_merge.RZMC)
        MEERA_data_RZMC.columns = ['MEERA_2']

        # Combining ERA5 and MEERA_2
        combine_df = pd.concat([ERA5_data_swvRZ.iloc[:7305], MEERA_data_RZMC], axis = 1)
        # Day Count Filter
        if(YCount == 5):
            combine_df = combine_df[0:1826]
        elif(YCount == 10):
            combine_df = combine_df[0:3652]
        elif(YCount == 15):
            combine_df = combine_df[0:5478]
        elif(YCount == 20):
            combine_df = combine_df[0:7304]
        else:
            combine_df = combine_df[:]
        
        mean = combine_df.mean(axis = 1)
        final_data = pd.concat([mean],axis= 1)
        final_data.columns = ['Mean']
        json_data = final_data.to_json()

        return json_data

class MeanNcarData_class(models.Model):
    def MeanNcarData_read_fun(self,request):
        # Conversion of request from django.core.handlers.wsgi.WSGIRequest to dict data type
        body_unicode = request.body.decode('utf-8')
        body = json.loads(body_unicode)
        print(body['lat'])
        lat = np.array(body['lat'])
        lon = np.array(body['lon'])
        site = np.array(body['SiteName'])
        YearCount = np.array(body['YearCount'])

        " ERA5 Soil Moisture Data "
        #Reading the netcdf file (.nc file) using xarray
        ds_ERA5 = xr.open_dataset("ERA5/ERA5_SM_daily_1999_2021_0_5m.nc", engine="netcdf4")

        #swvRZ variable (location of 5 Site)
        if site == 'TALL':
            ERA5 =ds_ERA5.swvRZ.sel(lat=32.950, lon=272.607, method='nearest')
        elif site == 'ABBY':
            ERA5 =ds_ERA5.swvRZ.sel(lat=45.762, lon=237.67, method='nearest')
        elif site == 'BLAN':
            ERA5 =ds_ERA5.swvRZ.sel(lat=39.060, lon=281.928, method='nearest')
        elif site == 'CLBJ':
            ERA5 =ds_ERA5.swvRZ.sel(lat=33.401, lon=262.430, method='nearest')
        elif site == 'WOOD':
            ERA5 =ds_ERA5.swvRZ.sel(lat=47.128, lon=260.759, method='nearest')
        else:
            ERA5 =ds_ERA5.swvRZ.sel(lat=32.950, lon=272.607, method='nearest')

        ERA5_data_merge = xr.merge([ERA5], compat = 'override')
        ERA5_data_swvRZ = pd.DataFrame(ERA5_data_merge.swvRZ)
        ERA5_data_swvRZ.columns = ['RootZone Soil Moisture']
        
        " MERRA_2 Soil Moisture Data "
        ds_MEERA_2 = xr.open_dataset("MEERA_2/MERRA2_SM_0_5m_1999_2018_organize.nc", engine="netcdf4")
        #RZMC variable (location of 5 Site)
        if site == 'TALL':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=32.950, lon=272.607, method='nearest')
        elif site == 'ABBY':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=45.762, lon=237.67, method='nearest')
        elif site == 'BLAN':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=39.060, lon=281.928, method='nearest')
        elif site == 'CLBJ':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=33.401, lon=262.430, method='nearest')
        elif site == 'WOOD':
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=47.128, lon=260.759, method='nearest')
        else:
            MEERA_2 =ds_MEERA_2.RZMC.sel(lat=32.950, lon=272.607, method='nearest')

        MEERA_data_merge = xr.merge([MEERA_2], compat = 'override')
        MEERA_data_RZMC = pd.DataFrame(MEERA_data_merge.RZMC)
        MEERA_data_RZMC.columns = ['Water Root Zone']

        " Combining ERA-5 and MEERA-2 data "
        combine = pd.concat([ERA5_data_swvRZ.iloc[:7305], MEERA_data_RZMC], axis = 1)

        " Mean Value for ERA-5 and MEERA-2 data "
        mean = combine.mean(axis = 1)
        final_data = pd.concat([mean],axis= 1)
        final_data.columns = ['Mean']

        " 2018 Jan month Mean "
        Mean_data = final_data[6939:6985]
        # Converting Mean_data index from [6939:6985] to [0:45]
        Mean_df = pd.DataFrame(np.array(Mean_data))
        Mean_df.columns = ['Mean']

        " NCAR H2SOI Data "
        #Reading the netcdf file (.nc file) using xarray
        ds_NACR = xr.open_dataset("Nc_data/cesm2cam6v2.2018-01-01.00.clm2.h0.2018-01-01-00000.nc", engine="netcdf4")

        # Converting into timestamp
        datetimeindex = ds_NACR.indexes['time'].to_datetimeindex()
        ds_NACR['time'] = datetimeindex
        date_df = pd.to_datetime(ds_NACR['time'])

        # Converting timestamp into DataFrame
        date_dataFrame = pd.DataFrame(date_df)
        date_dataFrame.columns = ['Date']

        #H2OSOI variable (location of 5 Site)
        if site == 'TALL':
            loc_1_H2OSOI=ds_NACR.H2OSOI.sel(lat=32.950, lon=272.607, method='nearest')
        elif site == 'ABBY':
            loc_1_H2OSOI=ds_NACR.H2OSOI.sel(lat=45.762, lon=237.67, method='nearest')
        elif site == 'BLAN':
            loc_1_H2OSOI=ds_NACR.H2OSOI.sel(lat=39.060, lon=281.928, method='nearest')
        elif site == 'CLBJ':
            loc_1_H2OSOI=ds_NACR.H2OSOI.sel(lat=33.401, lon=262.430, method='nearest')
        elif site == 'WOOD':
            loc_1_H2OSOI=ds_NACR.H2OSOI.sel(lat=47.128, lon=260.759, method='nearest')
        else:
            loc_1_H2OSOI=ds_NACR.H2OSOI.sel(lat=32.950, lon=272.607, method='nearest')

        data_H2OSOI = pd.DataFrame(loc_1_H2OSOI)
        data_H2OSOI_5 = data_H2OSOI.iloc[ :46 , : 5]
        H2OSOI_df_5_mean = data_H2OSOI_5.mean(axis = 1)

        H2OSOI_dataFrame = pd.DataFrame(H2OSOI_df_5_mean)
        H2OSOI_dataFrame.columns = ['H2OSOI']

        temp_df = pd.concat([date_dataFrame,H2OSOI_dataFrame,Mean_df], axis =1 )
        dataset = temp_df
        json_data = dataset.to_json()

        return json_data