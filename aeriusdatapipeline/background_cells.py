import pandas as pd
import numpy as np
import os
import time

from .lib_database_create import _create_id_col
from .lib_database_create import get_substance_id
from .export import Table


no2_path = './data/background_conc/'
nh3_path = './data/ammonia/'
dep_path = './data/background_cells/CBEDSingleYears/rCBED_Fdep_keqHaYr_1986-2\
                                                            020_gridavg.csv'


def get_background_cells_square(path=no2_path):
    '''takes in one of the 1x1km concentrations and calculates gridsquares
    from the centre points x and y. gives back a full df with info inc
    the id
    '''

    # this just gets the first in the conc dir
    # any of them would do
    conc_1 = os.listdir(path)[0]
    df_cell = pd.read_csv(path+conc_1, header=5)

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
    df_cell['geometry'] = 'POLYGON ((' + df_cell['LxLy'] + ', '\
        + df_cell['HxLy'] + ', ' + df_cell['HxHy'] + ', ' + df_cell['LxHy']\
        + ', ' + df_cell['LxLy'] + '))'

    df_cell = _create_id_col(df_cell, 'background_cell_id')
    df_cell['coords'] = df_cell['x'] + ' - ' + df_cell['y']

    return(df_cell[[
        'ukgridcode', 'x', 'y', 'geometry', 'background_cell_id', 'coords'
        ]])


def get_ammonia_data(path=nh3_path):
    '''takes in the path of the directory with ammonies conc
    files in and creates a df from all of them combined
    '''

    file_list = os.listdir(path)
    df = pd.DataFrame()
    # Read in the CSV and rename columns
    for file in file_list:
        df_ammonia = pd.read_csv(path + file)

        df_ammonia = df_ammonia.rename(columns={
            df_ammonia.columns[0]: "easting",
            df_ammonia.columns[1]: "northing",
            df_ammonia.columns[2]: "ammonia"
            })

        t_start = time.time()
        print('Started processing ' + file + '....')
        # converting the 5x5km grid to 1x1
        df = five_to_one_k(df_ammonia)
        t_end = time.time()
        print('....file finished in ' + str(t_end - t_start) + '\n\n')

    # Rename Dataframe columns
    ammonia = df.rename(
        columns={0: "x", 1: "y", 2: "concentration", 3: "year"}
        )
    ammonia["substance_id"] = get_substance_id('nh3')

    return(ammonia)


def get_concentration_data(dirname=no2_path):
    '''take the directory path with the concetration data for no2
    nox and so2 files in it and creates a single df from all
    of them
    '''

    # create vairables
    conc_data_list = []
    files = os.listdir(dirname)

    # loop through CSV files making changes to them and adding to list
    for conc_file in files:
        print(conc_file)
        df_conc_single = pd.read_csv(dirname + conc_file, header=5)
        df_conc_single["year"] = df_conc_single.columns[3][-4:]
        df_conc_single["substance_id"] = df_conc_single.columns[3][:3]
        df_conc_single = df_conc_single.rename(
            columns={df_conc_single.columns[3]: "concentration"}
            )
        # print(df_conc_single.head())
        conc_data_list.append(df_conc_single)

    # buildLarge DF from List of CSVs in conc_data_list
    df_conc_total = pd.concat(conc_data_list, ignore_index=True,)

    return(df_conc_total)


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


##########################


def create_table_background_cell():
    '''creates the table for the background cells. simply
    reducing the columns from get_background_cells_square
    '''

    df = get_background_cells_square()
    # removed for now so that all background squares exist
    # even with no conc or dep
    # df = df[df['background_cell_id'].isin(cell_list)]

    table = Table()
    table.data = df[['background_cell_id', 'geometry']]
    table.name = 'background_cell'
    return(table)


def create_table_background_concentration():
    '''creates the table for background concentrations by
    combining the data from nh3 as well as the no2/nox/so2 data
    '''

    df_cell = get_background_cells_square()
    df_conc = get_concentration_data()

    df_conc['substance_id'] = df_conc['substance_id'].replace({
        'no2': get_substance_id('no2'),
        'so2': get_substance_id('so2'),
        'nox': get_substance_id('nox'),
        'nh3': get_substance_id('nh3')
    })

    df_conc = pd.merge(df_conc, df_cell[['x', 'y', 'background_cell_id']],
                       how='left', on=['x', 'y'])

    df_ammonia = get_ammonia_data()

    df_ammonia = pd.merge(
        df_ammonia, df_cell[['x', 'y', 'background_cell_id']],
        how='left', on=['x', 'y']
        )

    # There are some background cells missing from the ammonia chart
    # get rid of the missing ones everywhere
    df_ammonia = df_ammonia[df_ammonia['background_cell_id'].notna()]
    data_cells = df_ammonia['background_cell_id'].tolist()
    df_conc = df_conc[df_conc['background_cell_id'].isin(data_cells)]

    df_conc_t = pd.concat([df_ammonia, df_conc])

    # df_conc_t['background_cell_id'] =\
    #                       df_conc_t['background_cell_id'].astype(str)
    # df_conc_t = df_conc_t.loc[df_conc_t['concentration'] != 'MISSING']

    table = Table()
    table.data = df_conc_t[[
        'background_cell_id', 'year', 'substance_id', 'concentration'
        ]]
    table.name = 'background_concentration'
    return(table)


def create_table_background_cell_depositions(filepath=dep_path):
    '''takes the direectory path for the depositions files ass input
    and outputs the the years back to 2017 as a single df
    '''

    df = pd.read_csv(filepath)

    df = df[df['year'].isin([2020, 2019, 2018, 2017])]

    df = five_to_one_k(df)

    # nitrogen deposition. for MVP this is the only one needed
    df['deposition'] = df['NOx'] + df['NHx']
    df['adep'] = df['NOx'] + df['NHx'] + df['SOx'] - df['CaMg']
    # gets rid of weird entries
    df['deposition'] = pd.to_numeric(df['deposition'], errors='coerce')
    # converting from keq to kg
    df['deposition'] = df['deposition'] * 14
    df = df.dropna(subset=['deposition'])

    df_cell = get_background_cells_square()
    df = pd.merge(df, df_cell[['x', 'y', 'background_cell_id']],
                  how='left', on=['x', 'y'])
    df = df[df['background_cell_id'].notna()]

    df['year'] = df['year'].astype(int)

    table = Table()
    table.data = df[['background_cell_id', 'year', 'deposition']]
    table.name = 'background_cell_depositions'
    return(table)
