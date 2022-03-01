import pandas as pd
import numpy as np
import os
import time
import winsound

import lib_database_create as libdb


def get_background_cells_square(path):

    # Import data
    df_cell = pd.read_csv(path, header=5)

    # add or subtract 500 from X and y to get grid corner  coordinates
    df_cell['x+500'] = df_cell['x']+500
    df_cell['x-500'] = df_cell['x']-500
    df_cell['y+500'] = df_cell['y']+500
    df_cell['y-500'] = df_cell['y']-500

    # after the +/- 500, they dont need to be nums anymore
    df_cell = df_cell.astype(str)
    # Concatenate the x+500, y+500, x-500 and y-500 into the points of the
    # gridsquares and turn them into strings
    df_cell['LxLy'] = df_cell['x-500'] + ' ' + df_cell['y-500']
    df_cell['HxLy'] = df_cell['x+500'] + ' ' + df_cell['y-500']
    df_cell['HxHy'] = df_cell['x+500'] + ' ' + df_cell['y+500']
    df_cell['LxHy'] = df_cell['x-500'] + ' ' + df_cell['y+500']
    # df_cell[['centre']] = df_cell['x'] + ' ' + df_cell['y']

    # Concatenate the center points into
    df_cell['geometry'] = 'POLYGON ((' + df_cell['LxLy'] + ', '\
        + df_cell['HxLy'] + ', ' + df_cell['HxHy'] + ', ' + df_cell['LxHy']\
        + ', ' + df_cell['LxLy'] + '))'

    df_cell = libdb._create_id_col(df_cell, 'background_cell_id')
    df_cell['coords'] = df_cell['x'] + ' - ' + df_cell['y']

    return(df_cell[[
        'ukgridcode', 'x', 'y', 'geometry', 'background_cell_id', 'coords'
        ]])


def get_ammonia_data(path):
    file_list = os.listdir(path)
    df = pd.DataFrame()
    # Read in the CSV and rename columns
    for file in file_list:
        t_start = time.time()
        print('Started processing ' + file + '....')
        df_ammonia = pd.read_csv(path + file)
        year = file[8:12]

        df_ammonia = df_ammonia.rename(columns={
            df_ammonia.columns[0]: "easting",
            df_ammonia.columns[1]: "northing",
            df_ammonia.columns[2]: "ammonia"
            })

        # Create list of eastings and northings
        # TODO replace with the function
        for i, row in df_ammonia.iterrows():
            # get row out of df_ammonia
            ammonia = row.ammonia
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
                new_row_set = [[east_val, northings[0], ammonia, year],
                               [east_val, northings[1], ammonia, year],
                               [east_val, northings[2], ammonia, year],
                               [east_val, northings[3], ammonia, year],
                               [east_val, northings[4], ammonia, year]]
                # Append Array to dataframe
                df = df.append(new_row_set)
        t_end = time.time()
        print('....file finished in ' + str(t_end - t_start) + '\n\n')
    # Rename Dataframe columns
    ammonia = df.rename(
        columns={0: "x", 1: "y", 2: "concentration", 3: "year"}
        )
    ammonia["substance_id"] = libdb.get_substance_id('nh3')

    return(ammonia)


def get_concentration_data(dirname):

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


def create_table_background_cell(df):

    # df = df[df['background_cell_id'].isin(cell_list)]
    return(df[['background_cell_id', 'geometry']])


def create_table_background_concentration(conc_path, ammonia_path):

    # just getting any of the no2 etc files to get background cells from
    conc_1 = os.listdir(conc_path)[0]
    df_cell = get_background_cells_square(conc_path+conc_1)

    df_conc = get_concentration_data(conc_path)

    df_conc['substance_id'] = df_conc['substance_id'].replace({
        'no2': libdb.get_substance_id('no2'),
        'so2': libdb.get_substance_id('so2'),
        'nox': libdb.get_substance_id('nox'),
        'nh3': libdb.get_substance_id('nh3')
    })

    df_conc = pd.merge(df_conc, df_cell[['x', 'y', 'background_cell_id']],
                       how='left', on=['x', 'y'])

    df_ammonia = get_ammonia_data(ammonia_path)

    df_ammonia = pd.merge(
        df_ammonia, df_cell[['x', 'y', 'background_cell_id']],
        how='left', on=['x', 'y']
        )

    # There are some ackground cells missing from the ammonia chart
    # get rid of the missing ones everywhere
    df_ammonia = df_ammonia[df_ammonia['background_cell_id'].notna()]
    data_cells = df_ammonia['background_cell_id'].tolist()
    df_conc = df_conc[df_conc['background_cell_id'].isin(data_cells)]

    df_conc_total = pd.concat([df_ammonia, df_conc])

    return(df_conc_total[[
        'background_cell_id', 'year', 'substance_id', 'concentration'
        ]])


def create_table_background_cell_depositions(filepath):

    df = pd.read_csv(filepath)

    df = df[df['year'].isin([2020, 2019, 2018, 2017])]

    df = five_to_one_k(df)

    # nitrogen deposition. for MVP this is the only one needed
    df['deposition'] = df['NOx'] + df['NHx']
    df['adep'] = df['NOx'] + df['NHx'] + df['SOx'] - df['CaMg']
    df['deposition'] = pd.to_numeric(df['deposition'], errors='coerce')
    # converting from keq to kg
    df['deposition'] = df['deposition'] * 14
    df = df.dropna(subset=['deposition'])

    df_cell = get_background_cells_square('./data/background_cells/background_squares/mapno22017.csv')
    df = pd.merge(df, df_cell[['x', 'y', 'background_cell_id']],
                  how='left', on=['x', 'y'])
    df = df[df['background_cell_id'].notna()]

    df['year'] = df['year'].astype(int)

    return(df[['background_cell_id', 'year', 'deposition']])


if __name__ == "__main__":

    # df_cell = get_background_cells_square('./data/background_conc/mapso22019.csv')
    # df_cell_table = create_table_background_cell(df_cell)
    # df_cell_table.to_csv('./output/aerius_data_22-01-21/background_cells.txt', sep='\t', index=False)

    # df_conc = create_table_background_concentration('./data/background_conc/', './data/ammonia/')
    # df_conc['background_cell_id'] = df_conc['background_cell_id'].astype(str)
    # df_conc = df_conc.loc[df_conc['concentration'] != 'MISSING']
    # df_conc.to_csv('./output/21-11-04-dataset/background_cell_concentrations.txt', sep='\t', index=False)

    # df_dep = create_table_background_cell_depositions('./data/background_cells/CBEDSingleYears/rCBED_Fdep_keqHaYr_1986-2020_gridavg.csv')
    # df_dep.to_csv('./output/aerius_data_22-02-07/background_cell_depositions.txt', sep='\t', index=False)

    print(get_background_cells_square('./data/background_cells/background_conc/mapno22017.csv'))

    winsound.Beep(2500, 1000)
