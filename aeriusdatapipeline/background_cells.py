# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 13:04:15 2022

@author: Ollie.Grint
"""
# import packages
import os
import pandas as pd
import csv
import numpy as np
from shapely import wkb, wkt
import geopandas as gpd
from pathlib import Path


pd.options.mode.chained_assignment = None

from .export import Table

###########################################################################################################################



# define filepaths for each of the data
NOx_Folder_England_and_Wales = 'data\\background depositions and concentrations\\NOX data\\England and Wales'
NOx_Folder_Scotland = 'data\\background depositions and concentrations\\NOX data\\Scotland'
NOx_Folder_NI = 'data\\background depositions and concentrations\\NOX data\\NI\\UK regions data'
Ammonia_Nitrogen_5KM_file_g = 'data\\background depositions and concentrations\\Ammonia and Nitrogen\\5km\\CBED-deposition-gridaverage-2017-2019.csv'
Ammonia_Nitrogen_5KM_file_f = 'data\\background depositions and concentrations\\Ammonia and Nitrogen\\5km\\CBED-deposition-forest-2017-2019.csv'
Ammonia_Nitrogen_5KM_file_m = 'data\\background depositions and concentrations\\Ammonia and Nitrogen\\5km\\CBED-deposition-moorland-2017-2019.csv'
NO2_Folder='data\\background depositions and concentrations\\NO2'
SO2_Folder='data\\background depositions and concentrations\\SO2'
hexagons_file='data/hexagons/hexagons_20220530.txt'

###########################################################################################################################

# define the range of years which will be used in the background years table (year options that can eb used in the aerius tool

background_year_start=2018
background_year_end=2030

###########################################################################################################################

# dictionaries for determining which years map to which in the target_background_years table
substance_7_year_dict={'2018':'2018','2019':'2019','2020':'2020','2021':'2021','2022':'2021','2023':'2021','2024':'2021','2025':'2021','2026':'2021','2027':'2021','2028':'2021',
                       '2029':'2021','2030':'2021'}
substance_11_year_dict={'2018':'2018','2019':'2019','2020':'2020','2021':'2021','2022':'2022','2023':'2023','2024':'2024','2025':'2025','2026':'2026','2027':'2027','2028':'2028',
                        '2029':'2029','2030':'2030'}
substance_17_year_dict={'2018':'2018','2019':'2018','2020':'2018','2021':'2018','2022':'2018','2023':'2018','2024':'2018','2025':'2018','2026':'2018','2027':'2018','2028':'2018',
                        '2029':'2018','2030':'2018'}
substance_1711_year_dict={'2018':'2018','2019':'2018','2020':'2018','2021':'2018','2022':'2018','2023':'2018','2024':'2018','2025':'2018','2026':'2018','2027':'2018','2028':'2018',
                          '2029':'2018','2030':'2018'}

###########################################################################################################################

# dictionary for deposition type
deposition_type_dict={'0':'Standard','1':'Forest','2':'Moorland','3':'Gridaverage'}

###########################################################################################################################
def five_to_one_k(df):
    ''' converts a 5x5km grid into a 1x1. the coordinate columns need
    to be called easting and northing
    '''
    df_extend = pd.DataFrame()
    for i, row in df.iterrows():
        # get row out of df_ammonia
        east = row.easting
        north = row.northing
        # Create Arrays of Esstings and northings buy subtracting or
        # adding numbers
        eastings = np.array([
            east-2000, east-1000, east, east+1000, east+2000],
            dtype=np.intc)
        northings = np.array([
            north-2000, north-1000, north, north+1000, north+2000],
            dtype=np.intc)

        # Create arrays of the 1k grid square
        for east_val in eastings:
            # Create array from lists above
            new_row_set = [[east_val, northings[0], *row.tolist()],
                           [east_val, northings[1], *row.tolist()],
                           [east_val, northings[2], *row.tolist()],
                           [east_val, northings[3], *row.tolist()],
                           [east_val, northings[4], *row.tolist()]]
            # Append Array to dataframe
            df_extend = df_extend.append(new_row_set)

    df_extend.columns = ['x', 'y', *df.columns]

    return(df_extend)

###########################################################################################################################

def load_wkt(x):
    try:
        return wkt.loads(x)
    except:
        print(f'Failed {x}')
        return None
    


def _create_id_col(df, id_colname):
    '''creates a column for the db tables with a unique ID'''
    # _check_unique_rows(df)

    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df[id_colname] = df.index
    df[id_colname] = df[id_colname] + 1
    df[id_colname] = df[id_colname].astype(str)
    return(df)

###########################################################################################################################

def get_background_cells_square(df_cell):
    '''takes in one of the 1x1km concentrations and calculates gridsquares
    from the centre points x and y. gives back a full df with info inc
    the id
    '''

    df_cell=df_cell[['x','y']]
    df_cell=df_cell.drop_duplicates()
    # add or subtract 500 from X and y to get grid corner  coordinates
    df_cell['x+500'] = df_cell['x']+500
    df_cell['x-500'] = df_cell['x']-500
    df_cell['y+500'] = df_cell['y']+500
    df_cell['y-500'] = df_cell['y']-500

    # after the +/- 500, they dont need to be nums anymore
    df_cell = df_cell.astype(str)
    # getting the four corners of the square
    df_cell['LxLy'] = df_cell['x-500'] + ' ' + df_cell['y-500']
    df_cell['HxLy'] = df_cell['x+500'] + ' ' + df_cell['y-500']
    df_cell['HxHy'] = df_cell['x+500'] + ' ' + df_cell['y+500']
    df_cell['LxHy'] = df_cell['x-500'] + ' ' + df_cell['y+500']

    # making it into a polygon to be read by postGIS
    df_cell['geometry'] = ('POLYGON ((' + df_cell['LxLy'] + ', ' \
                           + df_cell['HxLy'] + ', ' + df_cell['HxHy'] + ', ' + df_cell['LxHy'] \
                           + ', ' + df_cell['LxLy'] + '))').astype(str)

    df_cell = _create_id_col(df_cell, 'background_cell_id')
    df_cell['coords'] = df_cell['x'] + ' - ' + df_cell['y']

    return(df_cell[[
        'x', 'y', 'geometry', 'background_cell_id', 'coords'
    ]])

###########################################################################################################################

def extract_nox_files(folder):
    '''extract all the files from each location and add them to one dataset'''
    # list the files
    files= os.listdir(folder)
    # create a master dataframe
    master_df=pd.DataFrame()
    # loop through files in the folder
    for i in files:
        # read each of the files
        path=folder+'\\'+i
        df = pd.read_csv(path, skiprows=5)
        # extract the year from filename
        df['year']=i[-8:-4]
        # set the column names
        df.columns=['Local_Auth_Code', 'x', 'y', 'geo_area', 'EU_zone_agglom_01', 'Total_NOx', 'Motorway_in',
                    'Motorway_out', 'Trunk_A_Rd_in', 'Trunk_A_Rd_out', 'Primary_A_Rd_in', 'Primary_A_Rd_out',
                    'Minor_Rd+Cold_Start_in','Minor_Rd+Cold_Start_out','Industry_in','Industry_out',
                    'Domestic_in', 'Domestic_out', 'Aircraft_in', 'Aircraft_out', 'Rail_in',
                    'Rail_out', 'Other_in', 'Other_out', 'Point_Sources', 'Rural','year']
        master_df = pd.concat([master_df,df], axis=0)
    return(master_df)

def combine_3_files(master,df1,df2,df3):
    '''combines 3 files'''
    master = pd.concat([master,df1], axis=0)
    master = pd.concat([master,df2], axis=0)
    master = pd.concat([master,df3], axis=0)
    return(master)

def add_background_cell_id(grid,substance,grid_val):
    ''' adds background cell ids for each of the substances'''
    # set the data types for input to match


    grid['x']=grid['x'].astype(str)
    grid['y']=grid['y'].astype(str)
    substance['x']=substance['x'].astype(str)
    substance['y']=substance['y'].astype(str)
    grid['x'] = grid['x'].replace(r'\.0$', '', regex=True)
    grid['y'] = grid['y'].replace(r'\.0$', '', regex=True)
    substance['x'] = substance['x'].replace(r'\.0$', '', regex=True)
    substance['y'] = substance['y'].replace(r'\.0$', '', regex=True) 


    # merge the background grid wih the substance on the easting and northing
    merged_grid = pd.merge(substance, grid, how='outer', on=['x', 'y'],indicator=True)
    # rename columns
    # subset only the cells in the data being added
    merged_grid=merged_grid[merged_grid['_merge']!= 'right_only']

    # select wanted columns
    if grid_val=='t':
        merged_grid=merged_grid[['background_cell_id','concentration','year','type_id']]
    else:
        merged_grid=merged_grid[['background_cell_id','concentration','year']]

    return(merged_grid)

def extract_ammonia_nitrogen_1(file):
    '''Extract all the information from the ammonia and nitrogen files'''
    # load data
    Ammonia_Nitrogen = pd.read_csv(file)
    # set columns to correct types
    Ammonia_Nitrogen['x']=Ammonia_Nitrogen['x'].astype(int)
    Ammonia_Nitrogen['y']=Ammonia_Nitrogen['y'].astype(int)
    return(Ammonia_Nitrogen)

def extract_ammonia_nitrogen_5(file):
    '''Extract all the information from the ammonia and nitrogen files'''
    # load data
    Ammonia_Nitrogen = pd.read_csv(file)
    # set columns to correct types
    return(Ammonia_Nitrogen)


def extract_no2_or_so2(folder):
    '''Extract all the information from the specified files'''
    # get filepath
    file = os.listdir(folder)
    # create empty dataframe
    master_df =pd.DataFrame()
    # loop through file
    for i in file:
        # open each file
        path=folder+'\\'+i
        df = pd.read_csv(path, skiprows=5)
        # extract year from name
        df['year']=i[-8:-4]
        # set columns
        df.columns=['gridcode', 'x', 'y', 'concentration','year']
        df['concentration']=df['concentration'].astype(str)
        master_df = pd.concat([master_df,df], axis=0)
    return(master_df)

def extract_unique_grid(nox,so2,no2,ammon_nit):
    '''create a list with no duplicates of every square in the data'''
    # subset just x and y for each
    nox_unique=nox[['x','y']]
    no2_unique=no2[['x','y']]
    so2_unique=so2[['x','y']]
    #ammon_nit_unique=ammon_nit[['x','y']]
    ammon_nit_unique=ammon_nit[['easting','northing']]

    # make a blank pandas
    master = pd.DataFrame()
    # append these all together
    master = pd.concat([master,nox_unique], axis=0)
    master = pd.concat([master,no2_unique], axis=0)
    master = pd.concat([master,so2_unique], axis=0)
    master = pd.concat([master,ammon_nit_unique], axis=0)
    # drop duplicates
    master.drop_duplicates()
    return(master)


def fill_in_any_missing_cells_deposition(cell,grid,substance):
    '''works out if there are any missing cells and fills them in with missing for each year'''
    # unique list of cells in background grid
    unique_cell=cell
    # pull out what the substance id is
    # pull out what the year range is for this data
    substance['year']=substance['year'].astype(int)
    min_year=int(min(substance['year']))
    max_year=int(max(substance['year']))
    # create a blank dataframe which will be filled
    filled_in_cells=pd.DataFrame()
    # loop through years needed
    for i in range(min_year,max_year):
        substance_loop=substance[substance['year']==i]
        unique_cell_substance=substance_loop[['background_cell_id']].drop_duplicates()
        # merge the unique cells with the cells used in the substance data
        unique_cell_merge = pd.merge(unique_cell_substance, unique_cell, how='outer', on=['background_cell_id'],indicator=True)
        # extract cells which are in the background grid but not the substance
        unique_cell_missing = pd.DataFrame(unique_cell_merge[unique_cell_merge['_merge'] == 'right_only'])
        # if there are missing cells
        if not unique_cell_missing.empty:
            # compare the missing cells to the background grid to work out their background cell id
            unique_cell_missing=unique_cell_missing[['background_cell_id','geometry']]
            unique_cell_missing= pd.merge(unique_cell_missing, grid, how='outer', on=['geometry'],indicator=True)
            unique_cell_missing = pd.DataFrame(unique_cell_missing[unique_cell_missing['_merge'] == 'both'])
            # set these to missing
            unique_cell_missing['concentration']='MISSING'
            # provide them a substance id
            # subset only needed columns
            unique_cell_missing=unique_cell_missing[['background_cell_id_x','concentration']]
            # rename columns
            unique_cell_missing.columns=['background_cell_id','concentration']
            # copy the dataframe for each year and add the data in
            df = unique_cell_missing.copy()
            # set which year data is for
            df['year']=i
            # add each year data together
            filled_in_cells=pd.concat([filled_in_cells,df], axis=0)
    # add the filled in data to the original so dataset is now complete
    substance=pd.concat([substance,filled_in_cells], axis=0)
    return(substance)

def keep_highest_duplicate(df):
    '''keep only the highest value from each cell for each year'''
    # convert year type
    df['year']=df['year'].astype(int)
    # calculate min and max year
    min_year=int(min(df['year']))
    max_year=int(max(df['year']))
    # create a blank dataframe which will be filled
    master=pd.DataFrame()
    # loop through years needed
    for i in range(min_year,max_year+1):
        # subset each year
        df_year=df[df['year']==i]
        # remove duplicates from that year, keeping the highest values
        df_year=df_year.sort_values('concentration').drop_duplicates('background_cell_id', keep='last')
        # add each year data together
        master=pd.concat([master,df_year], axis=0)
    return(master)

def remove_unneeded_grids(df_1,df_5):
    L5_known_and_unknown = pd.merge(original_L5_data_cleaned, L5_knowns_unknowns, how='outer', on=['Level_5', 'Pressure','SubregionName','Resistance'],indicator=True)
    merged = pd.DataFrame(df_1[df_5['_merge'] == 'left_only'])

def create_background_grid_table(output_c):
# extract each of the information for each dataset
    NOx_Eng_and_Wales=extract_nox_files(NOx_Folder_England_and_Wales)
    NOx_Scot=extract_nox_files(NOx_Folder_Scotland)
    NOx_NI=extract_nox_files(NOx_Folder_NI)
    NO2_df=extract_no2_or_so2(NO2_Folder)
    SO2_df=extract_no2_or_so2(SO2_Folder)
    Ammonia_Nitrogen_5km_g=extract_ammonia_nitrogen_5(Ammonia_Nitrogen_5KM_file_g)
    Ammonia_Nitrogen_1km_g=five_to_one_k(Ammonia_Nitrogen_5km_g)

    Ammonia_Nitrogen_5km_f=extract_ammonia_nitrogen_5(Ammonia_Nitrogen_5KM_file_f)
    Ammonia_Nitrogen_1km_f=five_to_one_k(Ammonia_Nitrogen_5km_f)

    Ammonia_Nitrogen_5km_m=extract_ammonia_nitrogen_5(Ammonia_Nitrogen_5KM_file_m)
    Ammonia_Nitrogen_1km_m=five_to_one_k(Ammonia_Nitrogen_5km_m)




    # create the empty dataframe for the nox information
    NOx =pd.DataFrame()
    # combine these files
    NOx = combine_3_files(NOx, NOx_Eng_and_Wales,NOx_Scot,NOx_NI)
    # subset the columns needed fopr the final Nox dataset
    NOx=NOx[['x','y','Total_NOx','year']]

    # subset substances for just one year
    Nox_single_year = NOx[NOx['year']=='2018']
    NO2_single_year = NO2_df[NO2_df['year']=='2018']
    SO2_single_year = SO2_df[SO2_df['year']=='2018']
    #Ammonia_Nitrogen_1km_single_year = Ammonia_Nitrogen_1km[Ammonia_Nitrogen_1km['SingleYear']=='2018']
    Ammonia_Nitrogen_1km_single_year = Ammonia_Nitrogen_1km_g
    # create a list with no duplicates of every square in the data
    unique_grid=extract_unique_grid(Nox_single_year,NO2_single_year,SO2_single_year,Ammonia_Nitrogen_1km_single_year)
    # use this to make the background grid of the uk
    background_grid=get_background_cells_square(unique_grid)

    # save temporary files
    print('Creating temporary files')
    Path(f'./output/{output_c}/temp').mkdir(parents=True, exist_ok=True)
    background_grid.to_csv(f'./output/{output_c}/temp/background_grid.csv')
    Ammonia_Nitrogen_1km_g.to_csv(f'./output/{output_c}/temp/Ammonia_Nitrogen_1km_g.csv')
    Ammonia_Nitrogen_1km_f.to_csv(f'./output/{output_c}/temp/Ammonia_Nitrogen_1km_f.csv')
    Ammonia_Nitrogen_1km_m.to_csv(f'./output/{output_c}/temp/Ammonia_Nitrogen_1km_m.csv')
    NO2_df.to_csv(f'./output/{output_c}/temp/NO2_df.csv')
    SO2_df.to_csv(f'./output/{output_c}/temp/SO2_df.csv')
    NOx.to_csv(f'./output/{output_c}/temp/NOx.csv')

    return(background_grid,NOx,Ammonia_Nitrogen_1km_g,Ammonia_Nitrogen_1km_f,Ammonia_Nitrogen_1km_m,NO2_df,SO2_df)


def create_background_grid(output_c):
    if output_c in os.listdir(f'./output/'):
        if 'temp' in os.listdir(f'./output/{output_c}/'):
            print('Temporary file found')
            background_grid = pd.read_csv(f'./output/{output_c}/temp/background_grid.csv')
            NOx = pd.read_csv(f'./output/{output_c}/temp/NOx.csv')
            Ammonia_Nitrogen_1km_g = pd.read_csv(f'./output/{output_c}/temp/Ammonia_Nitrogen_1km_g.csv')
            Ammonia_Nitrogen_1km_f = pd.read_csv(f'./output/{output_c}/temp/Ammonia_Nitrogen_1km_f.csv')
            Ammonia_Nitrogen_1km_m = pd.read_csv(f'./output/{output_c}/temp/Ammonia_Nitrogen_1km_m.csv')
            NO2_df = pd.read_csv(f'./output/{output_c}/temp/NO2_df.csv')
            SO2_df = pd.read_csv(f'./output/{output_c}/temp/SO2_df.csv')
            return(background_grid,NOx,Ammonia_Nitrogen_1km_g,Ammonia_Nitrogen_1km_f,Ammonia_Nitrogen_1km_m,NO2_df,SO2_df)
        else:
            print('No temporary file found')
            background_grid,NOx,Ammonia_Nitrogen_1km_g,Ammonia_Nitrogen_1km_f,Ammonia_Nitrogen_1km_m,NO2_df,SO2_df=create_background_grid_table(output_c)
            return(background_grid,NOx,Ammonia_Nitrogen_1km_g,Ammonia_Nitrogen_1km_f,Ammonia_Nitrogen_1km_m,NO2_df,SO2_df)

###########################################################################################################################
###########################################################################################################################
###########################################################################################################################
###########################################################################################################################


def create_table_deposition_type():

    df=pd.DataFrame(deposition_type_dict.items(), columns=['type_id', 'description'])
    return(df)




def create_background_cell_table(output_c):

    background_grid,NOx,Ammonia_Nitrogen_1km_g,Ammonia_Nitrogen_1km_f,Ammonia_Nitrogen_1km_m,NO2_df,SO2_df=create_background_grid(output_c)
    # subset the columns needed from gird to have a table with only backround cell id and polygon
    background_cell=background_grid[['background_cell_id','geometry']]




    return(background_cell)

def create_background_concentration_table(output_c):

    background_grid,NOx,Ammonia_Nitrogen_1km_g,Ammonia_Nitrogen_1km_f,Ammonia_Nitrogen_1km_m,NO2_df,SO2_df=create_background_grid(output_c)

    # subset the columns needed from gird to have a table with only backround cell id and polygon
    background_cell=background_grid[['background_cell_id','geometry']]

    NOx.rename(columns={'Total_NOx':'concentration'}, inplace=True)
    # add the background cell id to the substance table
    NOx=add_background_cell_id(background_grid,NOx,'f')

    # Keep only the highest value for each cell for each year
    NOx=keep_highest_duplicate(NOx)

    # add the substance id to the nox table
    NOx['substance_id']=11

    # provide a type id of standard
    NOx['type_id']='0'


    # subset ammonia
    #Ammonia = Ammonia_Nitrogen_1km[['x','y','NH3_ugm3','SingleYear']]
    Ammonia_g = Ammonia_Nitrogen_1km_g[['easting','northing','nhx_ga']]
    Ammonia_g['year']='2018'
    Ammonia_g['type_id']='3'
    Ammonia_g.rename(columns={'easting':'x','northing':'y','nhx_ga':'concentration'}, inplace=True)
   
    Ammonia_f = Ammonia_Nitrogen_1km_f[['easting','northing','nhx_f']]
    Ammonia_f['year']='2018'
    Ammonia_f['type_id']='1'
    Ammonia_f.rename(columns={'easting':'x','northing':'y','nhx_f':'concentration'}, inplace=True)
   

    Ammonia_m = Ammonia_Nitrogen_1km_m[['easting','northing','nhx_m']]
    Ammonia_m['year']='2018'
    Ammonia_m['type_id']='2'
    Ammonia_m.rename(columns={'easting':'x','northing':'y','nhx_m':'concentration'}, inplace=True)
    
    # Add tha background cell id to ammnonia
   
    Ammonia_g=add_background_cell_id(background_grid,Ammonia_g,'t')
    
    # Keep only the highest value for each cell for each year
    Ammonia_g=keep_highest_duplicate(Ammonia_g)
    
    # average all years (temp as dont have access to all data)
    #Ammonia=Ammonia.groupby(['background_cell_id'], as_index=False)['concentration'].apply(lambda x: x.sum())
    #Ammonia['concentration']=Ammonia['concentration']/3
    Ammonia_g['year']='2018'
   

    # add the substance id
    Ammonia_g['substance_id']=17

    # Add tha background cell id to ammnonia
    
    Ammonia_f=add_background_cell_id(background_grid,Ammonia_f,'t')
    
    # Keep only the highest value for each cell for each year
    Ammonia_f=keep_highest_duplicate(Ammonia_f)
   
    # average all years (temp as dont have access to all data)
    #Ammonia=Ammonia.groupby(['background_cell_id'], as_index=False)['concentration'].apply(lambda x: x.sum())
    #Ammonia['concentration']=Ammonia['concentration']/3

    Ammonia_f['year']='2018'
    

    # add the substance id
    Ammonia_f['substance_id']=17

    # Add tha background cell id to ammnonia 
    Ammonia_m=add_background_cell_id(background_grid,Ammonia_m,'t')
    
    # Keep only the highest value for each cell for each year
    Ammonia_m=keep_highest_duplicate(Ammonia_m)
    
    # average all years (temp as dont have access to all data)
    #Ammonia=Ammonia.groupby(['background_cell_id'], as_index=False)['concentration'].apply(lambda x: x.sum())
    #Ammonia['concentration']=Ammonia['concentration']/3

    Ammonia_m['year']='2018'
    
    # add the substance id
    Ammonia_m['substance_id']=17

 # merge
    Ammonia= pd.concat([Ammonia_m,Ammonia_f],ignore_index=True)
    Ammonia= pd.concat([Ammonia,Ammonia_g],ignore_index=True)


    NO2_df.rename(columns={'Total_NOx':'concentration'}, inplace=True)
    # subset columns of no2
    NO2_df=NO2_df[['x','y','concentration','year']]
    # Add tha background cell id to no2
    NO2_df=add_background_cell_id(background_grid,NO2_df,'f')
    # Keep only the highest value for each cell for each year
    NO2_df=NO2_df[NO2_df['concentration']!='MISSING']
    NO2_df['concentration']=NO2_df['concentration'].astype(float)
    NO2_df=keep_highest_duplicate(NO2_df)
    # add substance id to no2
    NO2_df['substance_id']=7
    NO2_df['type_id']='0'
    
    
    # subset columns of so2
    SO2_df=SO2_df[['x','y','concentration','year']]
    # Add tha background cell id to so2
    SO2_df=add_background_cell_id(background_grid,SO2_df,'f')
    # Keep only the highest value for each cell for each year
    SO2_df=SO2_df[SO2_df['concentration']!='MISSING']
    SO2_df['concentration']=SO2_df['concentration'].astype(float)
    SO2_df=keep_highest_duplicate(SO2_df)
    # add substance id to so2
    SO2_df['substance_id']=1
    SO2_df['type_id']='0'
    
    # Fill in any missing cells for each substance
    # NOx=fill_in_any_missing_cells(background_cell,background_grid,NOx)
    # Ammonia=fill_in_any_missing_cells(background_cell,background_grid,Ammonia)
    # NO2_df=fill_in_any_missing_cells(background_cell,background_grid,NO2_df)
    # SO2_df=fill_in_any_missing_cells(background_cell,background_grid,SO2_df)

    # create an empty dataframe for the background concentration table
    background_concentration=pd.DataFrame()

    # append all the tables together into it
    background_concentration=pd.concat([NOx,background_concentration], axis=0)
    background_concentration=pd.concat([SO2_df,background_concentration], axis=0)
    background_concentration=pd.concat([NO2_df,background_concentration], axis=0)
    background_concentration=pd.concat([Ammonia,background_concentration], axis=0)
    # set the year type
    
    # reorder the columns
    background_concentration=background_concentration[['background_cell_id','year','concentration','substance_id','type_id']]
    background_concentration['concentration']=background_concentration['concentration'].astype(str)
    background_concentration.sort_values(by='concentration', ascending=False)
    background_concentration=background_concentration.drop_duplicates(subset=["background_cell_id", "year","substance_id","type_id"], keep="first")
    background_concentration=background_concentration[['background_cell_id','year','substance_id','concentration','type_id']]
    background_concentration=background_concentration[background_concentration['concentration']!= 'MISSING']
    background_concentration['substance_id']=background_concentration['substance_id'].apply(lambda x: int(x))
    background_concentration['year']=background_concentration['year'].apply(lambda x: int(x))
    background_concentration=background_concentration.dropna()
    

    return(background_concentration)


def create_background_cell_depositions_table(output_c):

    background_grid,NOx,Ammonia_Nitrogen_1km_g,Ammonia_Nitrogen_1km_f,Ammonia_Nitrogen_1km_m,NO2_df,SO2_df=create_background_grid(output_c)

    # subset the columns needed from gird to have a table with only backround cell id and polygon
    background_cell=background_grid[['background_cell_id','geometry']]

    NOx.rename(columns={'Total_NOx':'concentration'}, inplace=True)
    # add the background cell id to the substance table
    NOx=add_background_cell_id(background_grid,NOx,'f')

    # Keep only the highest value for each cell for each year
    NOx=NOx[NOx['concentration']!='MISSING']
    NOx['concentration']=NOx['concentration'].astype(float)
    NOx=keep_highest_duplicate(NOx)

    # add the substance id to the nox table
    NOx['substance_id']=11
    NOx['type_id']='0'


    # subset ammonia
    #Ammonia = Ammonia_Nitrogen_1km[['x','y','NH3_ugm3','SingleYear']]
    Ammonia_g = Ammonia_Nitrogen_1km_g[['easting','northing','nhx_ga']]
    Ammonia_g['year']='2018'
    Ammonia_g['type_id']='3'
    Ammonia_g.rename(columns={'easting':'x','northing':'y','nhx_ga':'concentration'}, inplace=True)

    Ammonia_f = Ammonia_Nitrogen_1km_f[['easting','northing','nhx_f']]
    Ammonia_f['year']='2018'
    Ammonia_f['type_id']='1'
    Ammonia_f.rename(columns={'easting':'x','northing':'y','nhx_f':'concentration'}, inplace=True)

    Ammonia_m = Ammonia_Nitrogen_1km_m[['easting','northing','nhx_m']]
    Ammonia_m['year']='2018'
    Ammonia_m['type_id']='2'
    Ammonia_m.rename(columns={'easting':'x','northing':'y','nhx_m':'concentration'}, inplace=True)



    # Add tha background cell id to ammnonia
    Ammonia_g=add_background_cell_id(background_grid,Ammonia_g,'t')
    # Keep only the highest value for each cell for each year
    Ammonia_g['concentration']=Ammonia_g['concentration'].astype(float)
    Ammonia_g=keep_highest_duplicate(Ammonia_g)

    # average all years (temp as dont have access to all data)
    #Ammonia=Ammonia.groupby(['background_cell_id'], as_index=False)['concentration'].apply(lambda x: x.sum())
    #Ammonia['concentration']=Ammonia['concentration']/3
    Ammonia_g['year']='2018'

    # add the substance id
    Ammonia_g['substance_id']=17


    # Add tha background cell id to ammnonia
    Ammonia_f=add_background_cell_id(background_grid,Ammonia_f,'t')
    # Keep only the highest value for each cell for each year
    Ammonia_f['concentration']=Ammonia_f['concentration'].astype(float)
    Ammonia_f=keep_highest_duplicate(Ammonia_f)

    # average all years (temp as dont have access to all data)
    #Ammonia=Ammonia.groupby(['background_cell_id'], as_index=False)['concentration'].apply(lambda x: x.sum())
    #Ammonia['concentration']=Ammonia['concentration']/3
    Ammonia_f['year']='2018'

    # add the substance id
    Ammonia_f['substance_id']=17


    # Add tha background cell id to ammnonia
    Ammonia_m=add_background_cell_id(background_grid,Ammonia_m,'t')
    # Keep only the highest value for each cell for each year
    Ammonia_m['concentration']=Ammonia_m['concentration'].astype(float)
    Ammonia_m=keep_highest_duplicate(Ammonia_m)

    # average all years (temp as dont have access to all data)
    #Ammonia=Ammonia.groupby(['background_cell_id'], as_index=False)['concentration'].apply(lambda x: x.sum())
    #Ammonia['concentration']=Ammonia['concentration']/3
    Ammonia_m['year']='2018'

    # add the substance id
    Ammonia_m['substance_id']=17


    # merge
    Ammonia= pd.concat([Ammonia_m,Ammonia_f],ignore_index=True)
    Ammonia= pd.concat([Ammonia,Ammonia_g],ignore_index=True)

    # calculate the nitrogen deposition
    #Ammonia_Nitrogen_1km['Nitrogen_deposition']=Ammonia_Nitrogen_1km['NOx']+Ammonia_Nitrogen_1km['NHx']
    Ammonia_Nitrogen_1km_g['nox_ga'] = Ammonia_Nitrogen_1km_g['nox_ga'].fillna(0)
    Ammonia_Nitrogen_1km_g['nhx_ga'] = Ammonia_Nitrogen_1km_g['nhx_ga'].fillna(0)
    Ammonia_Nitrogen_1km_g['Nitrogen_deposition']=Ammonia_Nitrogen_1km_g['nox_ga']+Ammonia_Nitrogen_1km_g['nhx_ga']
    Ammonia_Nitrogen_1km_g['type_id']='3'
    Ammonia_Nitrogen_1km_m['nox_m'] = Ammonia_Nitrogen_1km_m['nox_m'].fillna(0)
    Ammonia_Nitrogen_1km_m['nhx_m'] = Ammonia_Nitrogen_1km_m['nhx_m'].fillna(0)
    Ammonia_Nitrogen_1km_m['Nitrogen_deposition']=Ammonia_Nitrogen_1km_m['nox_m']+Ammonia_Nitrogen_1km_m['nhx_m']
    Ammonia_Nitrogen_1km_m['type_id']='2'
    Ammonia_Nitrogen_1km_f['nox_f'] = Ammonia_Nitrogen_1km_f['nox_f'].fillna(0)
    Ammonia_Nitrogen_1km_f['nhx_f'] = Ammonia_Nitrogen_1km_f['nhx_f'].fillna(0)
    Ammonia_Nitrogen_1km_f['Nitrogen_deposition']=Ammonia_Nitrogen_1km_f['nox_f']+Ammonia_Nitrogen_1km_f['nhx_f']
    Ammonia_Nitrogen_1km_f['type_id']='1'

    # Convert to kg from keg
    Ammonia_Nitrogen_1km_g['Nitrogen_deposition']=Ammonia_Nitrogen_1km_g['Nitrogen_deposition']*14

    # subset just the nitrogen deposition
    #Nitrogen_deposition = Ammonia_Nitrogen_1km[['x','y','Nitrogen_deposition','SingleYear']]
    Ammonia_Nitrogen_1km_g = Ammonia_Nitrogen_1km_g[['easting','northing','Nitrogen_deposition','type_id']]
    Ammonia_Nitrogen_1km_g['year']='2018'
    Ammonia_Nitrogen_1km_g.rename(columns={'easting':'x','northing':'y','Nitrogen_deposition':'concentration'}, inplace=True)

    # Add tha background cell id to nitrogen deposition
    Ammonia_Nitrogen_1km_g=add_background_cell_id(background_grid,Ammonia_Nitrogen_1km_g,'t')

    # Keep only the highest value for each cell for each year
    Ammonia_Nitrogen_1km_g['concentration']=Ammonia_Nitrogen_1km_g['concentration'].astype(float)
    Ammonia_Nitrogen_1km_g=keep_highest_duplicate(Ammonia_Nitrogen_1km_g)

    #Nitrogen_deposition=Nitrogen_deposition.groupby('background_cell_id', as_index=False)['concentration'].apply(lambda x: x.sum())
    #Nitrogen_deposition['concentration']=Nitrogen_deposition['concentration']/3
    Ammonia_Nitrogen_1km_g['year']='2018'

    # Convert to kg from keg
    Ammonia_Nitrogen_1km_f['Nitrogen_deposition']=Ammonia_Nitrogen_1km_f['Nitrogen_deposition']*14

    # subset just the nitrogen deposition
    #Nitrogen_deposition = Ammonia_Nitrogen_1km[['x','y','Nitrogen_deposition','SingleYear']]
    Ammonia_Nitrogen_1km_f = Ammonia_Nitrogen_1km_f[['easting','northing','Nitrogen_deposition','type_id']]
    Ammonia_Nitrogen_1km_f['year']='2018'
    Ammonia_Nitrogen_1km_f.rename(columns={'easting':'x','northing':'y','Nitrogen_deposition':'concentration'}, inplace=True)

    # Add tha background cell id to nitrogen deposition
    Ammonia_Nitrogen_1km_f=add_background_cell_id(background_grid,Ammonia_Nitrogen_1km_f,'t')

    # Keep only the highest value for each cell for each year
    Ammonia_Nitrogen_1km_f['concentration']=Ammonia_Nitrogen_1km_f['concentration'].astype(float)
    Ammonia_Nitrogen_1km_f=keep_highest_duplicate(Ammonia_Nitrogen_1km_f)

    #Nitrogen_deposition=Nitrogen_deposition.groupby('background_cell_id', as_index=False)['concentration'].apply(lambda x: x.sum())
    #Nitrogen_deposition['concentration']=Nitrogen_deposition['concentration']/3
    Ammonia_Nitrogen_1km_f['year']='2018'

    # Convert to kg from keg
    Ammonia_Nitrogen_1km_m['Nitrogen_deposition']=Ammonia_Nitrogen_1km_m['Nitrogen_deposition']*14

    # subset just the nitrogen deposition
    #Nitrogen_deposition = Ammonia_Nitrogen_1km[['x','y','Nitrogen_deposition','SingleYear']]
    Ammonia_Nitrogen_1km_m = Ammonia_Nitrogen_1km_m[['easting','northing','Nitrogen_deposition','type_id']]
    Ammonia_Nitrogen_1km_m['year']='2018'
    Ammonia_Nitrogen_1km_m.rename(columns={'easting':'x','northing':'y','Nitrogen_deposition':'concentration'}, inplace=True)

    # Add tha background cell id to nitrogen deposition
    Ammonia_Nitrogen_1km_m=add_background_cell_id(background_grid,Ammonia_Nitrogen_1km_m,'t')

    # Keep only the highest value for each cell for each year
    Ammonia_Nitrogen_1km_m['concentration']=Ammonia_Nitrogen_1km_m['concentration'].astype(float)
    Ammonia_Nitrogen_1km_m=keep_highest_duplicate(Ammonia_Nitrogen_1km_m)


    #Nitrogen_deposition=Nitrogen_deposition.groupby('background_cell_id', as_index=False)['concentration'].apply(lambda x: x.sum())
    #Nitrogen_deposition['concentration']=Nitrogen_deposition['concentration']/3
    Ammonia_Nitrogen_1km_m['year']='2018'

    Ammonia_Nitrogen_1km=pd.concat([Ammonia_Nitrogen_1km_g,Ammonia_Nitrogen_1km_m],ignore_index=True)
    Ammonia_Nitrogen_1km= pd.concat([Ammonia_Nitrogen_1km,Ammonia_Nitrogen_1km_f],ignore_index=True)
    Ammonia_Nitrogen_1km['concentration'] = Ammonia_Nitrogen_1km['concentration'].replace('', np.nan)
    Nitrogen_deposition=Ammonia_Nitrogen_1km


    # subset columns of no2
    NO2_df=NO2_df[['x','y','concentration','year']]
    # Add tha background cell id to no2
    NO2_df=add_background_cell_id(background_grid,NO2_df,'f')
    # Keep only the highest value for each cell for each year
    NO2_df=NO2_df[NO2_df['concentration']!='MISSING']
    NO2_df['concentration']=NO2_df['concentration'].astype(float)
    NO2_df=keep_highest_duplicate(NO2_df)
    # add substance id to no2
    NO2_df['substance_id']=7
    NO2_df['type_id']='0'

    # subset columns of so2
    SO2_df=SO2_df[['x','y','concentration','year']]
    # Add tha background cell id to so2
    SO2_df=add_background_cell_id(background_grid,SO2_df,'f')
    # Keep only the highest value for each cell for each year
    SO2_df=SO2_df[SO2_df['concentration']!='MISSING']
    SO2_df['concentration']=SO2_df['concentration'].astype(float)
    SO2_df=keep_highest_duplicate(SO2_df)
    # add substance id to so2
    SO2_df['substance_id']=1
    SO2_df['type_id']='0'


    # Fill in any missing cells for each substance
    # Nitrogen_deposition=fill_in_any_missing_cells_deposition(background_cell,background_grid,Nitrogen_deposition)

    # create the background_cell_depositions
    background_cell_depositions=Nitrogen_deposition
    # set the correct data type
    background_cell_depositions['year']=background_cell_depositions['year'].astype(int)
    # reorder and rename the columns
    #background_cell_depositions=background_cell_depositions[['background_cell_id','year','concentration']]
    background_cell_depositions=background_cell_depositions[['background_cell_id','concentration','year','type_id']]
    background_cell_depositions.rename(columns={'concentration':'deposition'}, inplace=True)


    return(background_cell_depositions)




def create_background_years_table():
    background_years=list(range(background_year_start,background_year_end+1))
    background_years = pd.DataFrame({'background_year':background_years})

    deposition_type=create_table_deposition_type()

    deposition_type['type_id']=deposition_type['type_id'].astype(np.int64)
    
    print('Completed background_deposition_type')
    print(deposition_type.head(10))
    table2=Table()
    table2.data=deposition_type
    table2.name='background_deposition_type'

    background_years['background_year']=background_years['background_year'].astype(np.int64)

    print('Completed background_years')
    print(background_years.head(10))
    table = Table()
    table.data = background_years
    table.name = 'background_years'
    return(table,table2)


def create_target_background_years_table():
    substance_7_years = pd.DataFrame(substance_7_year_dict.items())
    substance_11_years = pd.DataFrame(substance_11_year_dict.items())
    substance_17_years = pd.DataFrame(substance_17_year_dict.items())
    substance_1711_years = pd.DataFrame(substance_1711_year_dict.items())

    substance_7_years.columns=['year','background_year']
    substance_11_years.columns=['year','background_year']
    substance_17_years.columns=['year','background_year']
    substance_1711_years.columns=['year','background_year']

    substance_7_years['emission_result_type']='concentration'
    substance_11_years['emission_result_type']='concentration'
    substance_17_years['emission_result_type']='concentration'
    substance_1711_years['emission_result_type']='deposition'

    substance_7_years['substance_id']='7'
    substance_11_years['substance_id']='11'
    substance_17_years['substance_id']='17'
    substance_1711_years['substance_id']='1711'

    target_background_years=pd.concat([substance_7_years,substance_11_years])
    target_background_years=pd.concat([target_background_years,substance_17_years])
    target_background_years=pd.concat([target_background_years,substance_1711_years])

    target_background_years=target_background_years[['year','emission_result_type','substance_id','background_year']]

    target_background_years['year']=target_background_years['year'].astype(np.int64)
    target_background_years['background_year']=target_background_years['background_year'].astype(np.int64)
    target_background_years['substance_id']=target_background_years['substance_id'].astype(np.int64)

    print('Completed target_background_years')
    print(target_background_years.head(10))
    table = Table()
    table.data = target_background_years
    table.name = 'target_background_years'
    return(table)

def coverage_fraction(bounding_box,geometry_):
    intersect_area=bounding_box.intersection(geometry_).area
    hex_area=bounding_box.area
    fraction=intersect_area/hex_area
    return(fraction)

def create_background_cell(output_c):
    df_cell_table = create_background_cell_table(output_c)
    df_cell_table=df_cell_table[df_cell_table['geometry']!='POLYGON ((nan nan, nan nan, nan nan, nan nan, nan nan))']

    df_cell_table['geometry'] = df_cell_table['geometry'].apply(lambda x: load_wkt(x))
    df_cell_table['geometry'] = df_cell_table['geometry'].apply(lambda x: wkb.dumps(x, hex=True, srid=27700))


    df_cell_table['background_cell_id']=df_cell_table['background_cell_id'].astype(np.int64)

    print('Completed background_cell')
    print(df_cell_table.head(10))
    table = Table()
    table.data = df_cell_table
    table.name = 'background_cell'
    return(table)


def create_background_concentration(output_c):
    df_conc = create_background_concentration_table(output_c)
    df_conc=df_conc[df_conc['substance_id']!=1]
    df_conc=df_conc[df_conc['substance_id']!='1']

    
    df_conc['background_cell_id']=df_conc['background_cell_id'].astype(np.int64)
    df_conc['year']=df_conc['year'].astype(np.int64)
    df_conc['substance_id']=df_conc['substance_id'].astype(np.int64)
    df_conc['type_id']=df_conc['type_id'].astype(np.int64)
   
    print('Completed background_concentration')
    print(df_conc.head(10))
    table = Table()
    table.data = df_conc
    table.name = 'background_concentration'
    return(table)


def create_background_cell_depositions(output_c):
    df_dep = create_background_cell_depositions_table(output_c)

    df_dep = df_dep[df_dep['background_cell_id'].notna()]
    df_dep['background_cell_id']=df_dep['background_cell_id'].astype(np.int64)
    df_dep['year']=df_dep['year'].astype(np.int64)
    df_dep['type_id']=df_dep['type_id'].astype(np.int64)

    print('Completed background_cell_deposition')
    print(df_dep.head(10))
    table = Table()
    table.data = df_dep
    table.name = 'background_cell_depositions'
    return(table)



def create_background_jurisdiction_policies(output_c):
    df_cell_table = create_background_cell_table(output_c)
    df_conc = pd.read_csv(f'./output/{output_c}/background_concentration.txt', sep="	")
    df_conc=df_conc[df_conc['substance_id']!=1]
    df_conc=df_conc[df_conc['substance_id']!='1']

    df_dep  = pd.read_csv(f'./output/{output_c}/background_cell_depositions.txt', sep="	")

    hexagons = pd.read_csv(hexagons_file, sep='\t', lineterminator='\r', header=None)
    hexagons.columns=('receptor_id','zoom_level','geometry')
    hexagons=hexagons[hexagons['zoom_level']==1]
    hexagons=hexagons.dropna()

    # this code converts hex to bounding box
    hexagons['bounding_box']=hexagons['geometry'].apply(lambda x: wkb.loads(x, hex=True))
    hexagons['bounding_box_2']=hexagons['bounding_box'].apply(lambda x: x.wkt)
    hexagons_geo = gpd.GeoDataFrame(hexagons, geometry=hexagons.bounding_box)


    cell=df_cell_table.dropna()
    cell=cell[cell['geometry']!='POLYGON ((nan nan, nan nan, nan nan, nan nan, nan nan))']

    cell['geometry_'] = cell['geometry'].apply(lambda x: load_wkt(x))
    cell_geo = gpd.GeoDataFrame(cell, geometry=cell.geometry_)

    # work out what the intercepts are between cell and hexagons
    intersection = cell_geo.overlay(hexagons_geo, how='intersection')
    intersection['coverage_fraction']=intersection[['bounding_box','geometry_']].apply(lambda x: coverage_fraction(*x), axis=1)

    intersection_sums=intersection.groupby('receptor_id', as_index=False)['coverage_fraction'].apply(lambda x: x.sum())
    intersection_sums.columns=['receptor_id','area_sum']
    intersection_merge=pd.merge(intersection, intersection_sums, how='outer', on=['receptor_id'],indicator=False)
    intersection_merge['proportion']=intersection_merge['coverage_fraction']/intersection_merge['area_sum']


    df_conc=df_conc[df_conc['concentration']!='MISSING']

    conc_hex_merge=pd.merge(df_conc,intersection_merge, how='outer', on=['background_cell_id'],indicator=True)

    conc_hex_merge=conc_hex_merge[conc_hex_merge['_merge']=='both']

    conc_hex_merge = conc_hex_merge.drop('_merge', axis=1)
    conc_hex_merge['concentration']=conc_hex_merge['concentration'].apply(lambda x: float(x))
    conc_hex_merge['background_concentration']=conc_hex_merge['coverage_fraction']*conc_hex_merge['concentration']
    hex_concentration=conc_hex_merge[['year','substance_id','receptor_id','background_concentration','type_id']]
    hex_concentration['emission_result_type']='concentration'
    hex_concentration.rename(columns={'year':'background_year','background_concentration':'result'}, inplace=True)
    hex_concentration=hex_concentration[['background_year','emission_result_type','substance_id','receptor_id','result','type_id']]

    hex_concentration=hex_concentration.groupby(['background_year','emission_result_type','substance_id','receptor_id','type_id'], as_index=False)['result'].apply(lambda x: x.sum())

    hex_concentration['result']=hex_concentration['result'].apply(lambda x: float(x))
    hex_concentration['result']=hex_concentration['result'].round(6)

    df_dep=df_dep[df_dep['deposition']!='MISSING']
    dep_hex_merge=pd.merge(df_dep,intersection_merge, how='outer', on=['background_cell_id'],indicator=True)
    dep_hex_merge=dep_hex_merge[dep_hex_merge['_merge']=='both']
    dep_hex_merge = dep_hex_merge.drop('_merge', axis=1)
    dep_hex_merge['deposition']=dep_hex_merge['deposition'].apply(lambda x: float(x))
    dep_hex_merge['background_concentration']=dep_hex_merge['coverage_fraction']*dep_hex_merge['deposition']
    dep_hex_merge['substance_id']=1711
    hex_deposition=dep_hex_merge[['year','substance_id','receptor_id','background_concentration','type_id']]
    hex_deposition['emission_result_type']='deposition'
    hex_deposition.rename(columns={'year':'background_year','background_concentration':'result'}, inplace=True)
    hex_deposition=hex_deposition[['background_year','emission_result_type','substance_id','receptor_id','result','type_id']]

    hex_deposition=hex_deposition.groupby(['background_year','emission_result_type','substance_id','receptor_id','type_id'], as_index=False)['result'].apply(lambda x: x.sum())

    hex_deposition['result']=hex_deposition['result'].apply(lambda x: float(x))
    hex_deposition['result']=hex_deposition['result'].round(6)

    dep_and_conc_hex = pd.concat([hex_concentration,hex_deposition], axis=0)

    dep_and_conc_hex['background_year']=dep_and_conc_hex['background_year'].apply(lambda x: int(x))
    dep_and_conc_hex['substance_id']=dep_and_conc_hex['substance_id'].apply(lambda x: int(x))

    dep_and_conc_hex=dep_and_conc_hex.dropna()
    dep_and_conc_hex['result']=dep_and_conc_hex['result'].apply(lambda x: np.format_float_positional(x, trim="-"))
    dep_and_conc_hex=dep_and_conc_hex.dropna()
    # dep_and_conc_hex['receptor_id']=dep_and_conc_hex['receptor_id'].apply(lambda x: np.format_float_positional(x, trim="-"))

    # dep_and_conc_hex = dep_and_conc_hex.replace(r'\n','', regex=True)
    # dep_and_conc_hex = dep_and_conc_hex.replace('"','', regex=True)
    # dep_and_conc_hex['receptor_id']=dep_and_conc_hex['receptor_id'].apply(lambda x: np.format_float_positional(x, trim="-"))
    dep_and_conc_hex['receptor_id']=dep_and_conc_hex['receptor_id'].apply(lambda x: int(x))
    dep_and_conc_hex=dep_and_conc_hex.dropna()

    dep_and_conc_hex['type_id']=dep_and_conc_hex['type_id'].astype(np.int64)
    dep_and_conc_hex['background_year']=dep_and_conc_hex['background_year'].astype(np.int64)
    dep_and_conc_hex['substance_id']=dep_and_conc_hex['substance_id'].astype(np.int64)
    dep_and_conc_hex['receptor_id']=dep_and_conc_hex['receptor_id'].astype(np.int64)

    print('Completed receptor_background_results')
    print(dep_and_conc_hex.head(10))
    table = Table()
    table.data = dep_and_conc_hex
    table.name = 'receptor_background_results'

    return(table)