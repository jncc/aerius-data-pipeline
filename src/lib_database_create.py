import inspect # for finding the name of the function to pass as errors
import pyodbc
from pathlib import Path

import numpy as np
import pandas as pd

########################################################################
# Setting global paramaters
########################################################################



########################################################################
# Internal data handling functions
########################################################################

def _create_dictionary_from_df(df):
    '''creates a dictionary for name swaps beteen the code and the name.
    Takes a dictionary with only two columns as argument. The first col
    will be the keys, the second will be the values
    '''

    df = df.drop_duplicates()
    # creating the dictionary from the two columns of the dataframe
    # to use to_dict, the index should be the keys
    feat_name_dict = pd.Series(df[df.columns[1]].values, 
                               index=df[df.columns[0]]).to_dict()

    return(feat_name_dict)

def _create_id_col(df, id_colname):
    '''creates a column for the db tables with a unique ID'''

    #_check_unique_rows(df)

    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df[id_colname] = df.index
    df[id_colname] = df[id_colname] + 1
    df[id_colname] = df[id_colname].astype(str)
    return(df)

def _convert_id(df_col, rename):
    '''converts colnames into substanc_id with dictionary'''

    for colname, substance_id in rename.items():
        df_col = df_col.replace(colname, substance_id)
    return(df_col)

########################################################################
# Internal data validation and QC functions
########################################################################

def _check_unique_rows(df):
    '''Checking that the dataframe has unique IDs (the individual rows
    in the postgresDB)
    '''

    cols = list(df.columns)
    # if these are the same then all rows are unique IDs
    if df.shape[0] == df.groupby(cols).ngroups:
        return()

    # inspect.stack()[1][3] gives the name of the function that called
    print('\n!!!!!!!Warning!!!!!!!!!!!!\n'\
        +str(df.shape[0] - df.groupby(cols).ngroups)\
        +' rows in '+inspect.stack()[2][3]+' are duplicated.\n')
    duplicate = df[df.duplicated(cols)]
    print(duplicate)

########################################################################
# Useful functions
########################################################################

def get_id(df, col_ref, col_id, name):
    return(df.loc[df[col_ref] == name, col_id].values[0])

def get_substance_id(name):
    '''a way to consistently get the right substance id'''
    df = create_table_substances()
    return(df.loc[df['name'] == name, 'substance_id'].values[0])

########################################################################

def create_table_substances():
    '''Creates the substance table. Takes no argument and returns a df'''

    # Data frame created like this to link the id with name
    # we dont want them getting mixed as more are added or taken away
    all_sub = np.array([[1, 'so2', 'sulphur dioxide'],
                        [2,	'co2',	'carbon dioxide'],
                        [4,	'co',	'carbon monoxide'],
                        [7,	'no2',	'nitrogen dioxide'],
                        [8,	'bap',	'benzo(a)pyrene'],
                        [9,	'c6h6',	'benzene'],
                        [10	,'pm10',	'particulate matter 10'],
                        [11,	'nox',	'nitrous oxides'],
                        [14, 'o3',	'ozone'],
                        [15,	'pm25',	'particulate matter 2.5'],
                        [17,	'nh3',	'ammonia'],
                        [98,	'fno2',	'nitryl fluoride'],
                        [99,	'cop98',	'carbon monoxide 98th percentile'],
                        [101,	'NOy',	'sum of all oxidized atmospheric odd-nitrogen species'],
                        [102,	'NHx',	'all ammonia reduction levels'],
                        [201,	'N',	'nitrogen'],
                        [18,	'ec',	'elemental carbon'],
                        [1711, 'ndep', 'nitrogen deposition'],
                        [1000, 'adep', 'acid deposition']])

    return(pd.DataFrame(all_sub, columns=['substance_id', 'name', 'description']))

def create_table_example_authorities():
    '''Creates the substance table. Takes no argument and returns a df'''

    # Data frame created like this to link the id with name
    # we dont want them getting mixed as more are added or taken away
    all_sub = np.array([[1003, 4, 'City of Belfast',	'City of Belfast', 'unknown'],
                        [1015, 1, 'Bedford (B)',	'Bedford (B)', 'unknown'],
                        [1108, 3, 'Gwynedd - Gwynedd', 'Gwynedd - Gwynedd', 'unknown'],
                        [1165, 2, 'Shetland Islands',	'Shetland Islands',	'unknown']])

    return(pd.DataFrame(all_sub, columns=['authority_id', 'country_id', 'code', 'name', 'type']))

def create_table_countries():
    '''Creates the table for countries, all manually put entered'''

    all_sub = np.array([[1, 'E', 'England'],
                        [2, 'S', 'Scotland'],
                        [3, 'W', 'Wales'],
                        [4, 'N', 'Northern Ireland'],
                        [5, 'SE', 'Scotland/England'],
                        [6, 'WE', 'Wales/England']])

    return(pd.DataFrame(all_sub, columns=['country_id', 'code', 'name']))