import os
import pandas as pd
import numpy as np

from .lib_database_create import get_substance_id
from .lib_database_create import _create_id_col
from .export import Table

#########################################################################
#                        global variables
#########################################################################

# TODO set unit tests on the funcs that use this making sure it doesn't change
housing_cats = ['Housing systems',
                'Grazing emissions',
                'Outdoor yards (collecting/feeding)']

root_path = './data/ag_emissions/all/'

animal_rename = {
    'Breeding birds': 'G4',
    'Breeding bulls': 'A7',
    'Breeding heifers': 'A3.1',
    'Breeding replacements': 'A3.2',
    'Broilers': 'E3',
    'Calves (<1 year)': 'A4',
    'Dairy calves (<1 year)': 'A5',
    'Dairy cows': 'A1',
    'Dairy in-calf heifers': 'A3.3',
    'Dairy replacements (>1 year, not in-calf)': 'A3.4',
    'Dry sows and gilts': 'D1.1',
    'Ducks and Geese': 'G1',
    'Ewes': 'B1.1',
    'Farmed deer': 'K4',
    'Female cows for slaughter': 'A2',
    'Finishers 20-80 kg': 'D3.1',
    'Finishers >80 kg': 'D3.2',
    'Goats': 'C1',
    'Growing pullets': 'E1',
    'Horses on agricultural holdings': 'K1',
    'Lambs': 'B1.2',
    'Laying hens': 'E2',
    'Male cows for slaughter': 'A6',
    'Rams': 'B1.3',
    'Turkeys': 'F1',
    'Weaners <20kg': 'D1.2',
    'Farrowing sows': 'D1.3',
    'Beef suckler cows': 'A3.5',
    'Boars': 'D2'
}

#########################################################################
#                        extract data
#########################################################################


def _create_housing_sub_df(df, section):
    '''Creates a subset of the main df for the different systems
    and mitigation systems. Only works for housing. Takes a string
    and returns a dataframe
    '''

    # Column Name variables
    animal_type = df.columns[0]
    country_year = df.columns[1]
    housing_system_emm = df.columns[3]
    year = country_year[-4:]

    # finds blank row indicies to find the data around them
    # needs to be an array for argmax function later
    blank_rows = np.array(df.index[df[animal_type].isna()].tolist())

    header_index = df.index[df[animal_type] == section].tolist()[0]
    next_blank_index = blank_rows[np.argmax(blank_rows > header_index)+1]

    subdf = df.iloc[header_index+2:next_blank_index]
    subdf_len = subdf.shape[0]

    df_final = pd.DataFrame({
        'farm_type': subdf[animal_type].tolist(),
        'source': [section]*subdf_len,
        'emmision_factor': subdf[housing_system_emm].tolist(),
        'animal': [animal_type]*subdf_len,
        'year': [year]*subdf_len
        })

    return(df_final)


def _create_sub_df(df, section):
    '''Creates a subset of the main df for the different systems
    and mitigation systems. Doesn't work on housing. Takes a string
    and returns a dataframe
    '''
    try:
        # Column Name variables
        animal_type = df.columns[0]
        country_year = df.columns[1]
        year = country_year[-4:]

        # finds blank row indicies to find the data around them
        # needs to be an array for argmax function later
        blank_rows = np.array(df.index[df[animal_type].isna()].tolist())

        header_index = df.index[df[animal_type] == section].tolist()[0]
        next_blank_index = blank_rows[np.argmax(blank_rows > header_index)]

        subdf = df.iloc[header_index+1:next_blank_index]
        subdf_len = subdf.shape[0]

        df_final = pd.DataFrame({
            'farm_type': subdf[animal_type].tolist(),
            'source': [section]*subdf_len,
            'emmision_factor': subdf[country_year].tolist(),
            'animal': [animal_type]*subdf_len,
            'year': [year]*subdf_len
            })

        return(df_final)
    except IndexError:
        return(pd.DataFrame())


def process_ag_emissions_data(file, path=root_path):
    '''Processes the entire ag emissions input data and generates two
    dataframes, one for the emissions factors and one for the mitigation
    factors. Takes a path to the data file as input
    '''
    df_full = pd.read_excel(path+file)

    # adds a blank line to the bottom to keep the section to blank line
    # logic the same below.
    df_full.loc[df_full.shape[0]] = [np.NaN] * df_full.shape[1]

    return(df_full)


def get_emissions(file):
    '''extracts the parts of the input data that give emission
    rates and combines them
    '''

    df_full = process_ag_emissions_data(file)

    # df_housing_em = _create_housing_sub_df(df_full, "Housing systems")
    df_housing_em = _create_sub_df(df_full, "Housing systems")
    df_grazing_em = _create_sub_df(df_full, "Grazing emissions")
    df_yard_em = _create_sub_df(df_full, "Outdoor yards (collecting/feeding)")
    df_storage_em = _create_sub_df(df_full, "Manure storage")
    df_spreading_em = _create_sub_df(df_full, "Manure spreading")

    df_em = pd.concat([df_housing_em, df_grazing_em, df_yard_em, df_storage_em,
                       df_spreading_em], axis=0)

    # return(df_em)
    # Just housing for MVP
    return(df_housing_em)


def get_reductions(file):
    '''extracts the part of the data sets that give reduction
    factors and combines them'''

    df_full = process_ag_emissions_data(file)

    df_housing_mit = _create_sub_df(df_full, "Housing mitigation")
    df_yard_mit = _create_sub_df(df_full, "Yard mitigation")
    df_storage_mit = _create_sub_df(df_full, "Storage mitigation")
    df_spreading_mit = _create_sub_df(df_full, "Spreading mitigation")

    df_mit = pd.concat([df_housing_mit, df_yard_mit, df_storage_mit,
                        df_spreading_mit], axis=0)

    # the col names in create_sub_df are named for emissions so need
    # to be renamed for reductions
    # return(df_mit.rename(columns={'emmision_factor': 'reduction_factor'}))
    # Just housing for MVP
    return(df_housing_mit.rename(columns={'emmision_factor': 'reduction_factor'}))


def get_full_df(get_func, path=root_path):
    '''goes throught he files and extracts the data. extracts
    emissions or reductions depending on the function used
    '''

    file_list = os.listdir(path)

    df_list = []
    for file in file_list:
        df = get_func(file)
        df_list.append(df)

    return(pd.concat(df_list))


def get_full_emissions():
    return(get_full_df(get_emissions))


def get_full_reductions():
    return(get_full_df(get_reductions))


#########################################################################
#                        helper functions
#########################################################################


def _assign_scrub(house):
    '''scrubber is a column we are using to signify the housing types
    that go to making up the 100% of animal time
    '''
    if house in housing_cats:
        return('t')
    return('f')


def set_unique_farm_name(df):
    '''used for IDs'''
    return(df['farm_type'] + ' - (' + df['animal'] + '/' + df['source'] + ')')


def create_farm_ids_dict(df_id):
    '''creates dicts to be used for converting IDs. the first
    output is id to val, the second is val to id
    '''

    # the code is used to create the id dictionary later
    # needs name and animal as some names are duplicated
    df_id['unique_name'] = set_unique_farm_name(df_id)
    df_id = _create_id_col(df_id, 'farm_id')

    id_to_val = dict(zip(df_id['farm_id'], df_id['unique_name']))
    val_to_id = {value: key for (key, value) in id_to_val.items()}

    return(id_to_val, val_to_id)


def insert_space(string):
    '''inserts a space in the code to give the name'''
    return string[0:1] + ' ' + string[1:]

#########################################################################
#                        create table functions
#########################################################################


def create_table_farm_animal_categories():
    '''creates a table of all the unique animal types from
    the datasets
    '''

    df = get_full_emissions()

    df_animal = pd.DataFrame({'description': df['animal'].unique()})

    df_animal = _create_id_col(df_animal, 'farm_animal_category_id')

    # the code should be a specific code for each animal
    # the lookup table comes from the dutch version
    df_animal['code'] = df_animal['description']
    df_animal['code'] = df_animal['code'].replace(animal_rename)
    # the name is the code but with a space after the initial letter
    df_animal['name'] = df_animal['code']
    df_animal['name'] = df_animal.apply(
        lambda df_animal: insert_space(df_animal['name']), axis=1
        )

    table = Table()
    table.data = df_animal[['farm_animal_category_id', 'code', 'name',
                            'description']]
    table.name = 'farm_animal_categories'
    return(table)


def create_table_farm_lodging_types():
    '''creates the farm lodging system table from the emission
    dataframe. uses the animal names dictionary as a second argument.
    returns a dataframe
    '''

    df = get_full_emissions()

    df_new = pd.DataFrame({
        'farm_animal_category_id': df['animal'],
        'type': df['source'],  # this will be used to filter
        'code': df['animal'],
        'emission': df['emmision_factor']  # for ordering the code
    })

    # the code is used to create the id dictionary later
    # needs name and animal as some names are duplicated
    df_new['description'] = set_unique_farm_name(df)
    df_new['scrubber'] = 'f'
    # df_new['scrubber'] = df_new.apply(
    #     lambda df_new: _assign_scrub(df_new['description']), axis=1
    #     )

    df_animal = create_table_farm_animal_categories().data
    # the actual name of the animal is in description
    animal_dict = dict(zip(df_animal['description'],
                           df_animal['farm_animal_category_id']))

    # replaceing the animal names with ids and using the dicitonary
    # to keep things consistent
    for name, id in animal_dict.items():
        df_new['farm_animal_category_id'] =\
                            df_new['farm_animal_category_id'].replace(name, id)
    # Creating a loding system id
    df_new = _create_id_col(df_new, 'farm_lodging_type_id')

    # the code comes from the animal type
    df_new['code'] = df_new['code'].replace(animal_rename)
    # the code then needs to have an extra part for the type of emission
    # the code starts at the most emissive source
    g = df_new.groupby('code')
    # adds the counting number suffix
    df_new['code'] += g.cumcount().add(1).astype(str).radd('.').\
        mask(g['code'].transform('count') == 1, '')

    # the name is the code but with a space after the initial letter
    df_new['name'] = df_new['code']
    df_new['name'] = df_new.apply(
        lambda df_new: insert_space(df_new['name']), axis=1
        )

    table = Table()
    table.data = df_new[[
        'farm_lodging_type_id', 'farm_animal_category_id', 'code',
        'name', 'description', 'scrubber'
        ]]
    table.name = 'farm_lodging_types'
    return(table)


def create_table_farm_reductive_lodging_system():
    '''creates the farm reductive lodging system tables. requires the
    mitigation total dataframe and the animal dictionary as inputs.
    retunrs a dataframe
    '''

    df = get_full_reductions()

    df_new = pd.DataFrame({
        'farm_animal_category_id': df['animal'],
        'type': df['source'],  # this will be used to filter
        'code': df['animal'],
        'emission': df['reduction_factor']  # for ordering the code
    })

    # the code is used to create the id dictionary later
    # needs name and animal as some names are duplicated
    df_new['description'] = set_unique_farm_name(df)
    df_new['scrubber'] = 'f'
    # df_new['scrubber'] = df_new.apply(
    #     lambda df_new: _assign_scrub(df_new['description']), axis=1
    #     )

    df_animal = create_table_farm_animal_categories().data
    # the actual name of the animal is in description
    animal_dict = dict(zip(df_animal['description'],
                           df_animal['farm_animal_category_id']))

    # replaceing the animal names with ids and using the dictionary
    # to keep things consistent
    for name, id in animal_dict.items():
        df_new['farm_animal_category_id'] =\
                        df_new['farm_animal_category_id'].replace(name, id)
    # creating a mitigation system id
    df_new = _create_id_col(df_new, 'farm_reductive_lodging_system_id')
    
    # the code comes from the animal type
    df_new['code'] = df_new['code'].replace(animal_rename)
    # the code then needs to have an extra part for the type of emission
    # the code starts at the most emissive source
    g = df_new.groupby('code')
    # adds the counting number suffix
    df_new['code'] += g.cumcount().add(1).astype(str).radd('.').\
        mask(g['code'].transform('count') == 1, '')

    # the name is the code but with a space after the initial letter
    df_new['name'] = df_new['code']
    df_new['name'] = df_new.apply(
        lambda df_new: insert_space(df_new['name']), axis=1
        )
    
    table = Table()
    table.data = df_new[[
        'farm_reductive_lodging_system_id', 'farm_animal_category_id', 'code',
        'name', 'description', 'scrubber'
        ]]
    table.name = 'farm_reductive_lodging_system'
    return(table)


def create_table_farm_lodging_type_emission_factors():
    '''emissions factors for each lodging type'''

    df = get_full_emissions()
    # getting the ids in the same way for each table
    # df_em_total['unique_name'] = set_unique_farm_name(df)
    df['farm_lodging_type_id'] = set_unique_farm_name(df)
    df = df.replace(create_farm_ids_dict(get_full_emissions())[1])

    # converting from kg to g
    df['emmision_factor'] = df['emmision_factor'] * 1000

    df['substance_id'] = get_substance_id('nh3')

    table = Table()
    table.data = df[['farm_lodging_type_id', 'substance_id',
                     'emmision_factor']]
    table.name = 'farm_lodging_type_emission_factors'
    return(table)


def create_table_farm_reductive_lodging_system_reduction_factor():
    '''reductions factors for each reductive system'''

    df_mit = get_full_reductions()
    df_mit['farm_reductive_lodging_system_id'] = set_unique_farm_name(df_mit)
    # df['farm_reductive_lodging_system_id'] = \
    #     df['farm_type'] + ' - ' + df['animal']
    df_mit = df_mit.replace(create_farm_ids_dict(get_full_reductions())[1])

    df_mit['reduction_factor'] = df_mit['reduction_factor'] / 100

    df_mit['substance_id'] = get_substance_id('nh3')

    table = Table()
    table.data = df_mit[[
        'farm_reductive_lodging_system_id', 'substance_id', 'reduction_factor'
        ]]
    table.name = 'farm_reductive_lodging_system_reduction_factor'
    return(table)


def create_table_farm_lodging_types_to_reductive_lodging_systems():
    '''which reductive factors are applicable to which
    emission factors
    '''

    df_em = get_full_emissions()
    df_mit = get_full_reductions()

    df_em['unique_name'] = set_unique_farm_name(df_em)
    df_mit['unique_name'] = set_unique_farm_name(df_mit)

    mitigations_join = df_mit.replace({
        'Housing mitigation': 'Housing systems',
        'Storage mitigation': 'Manure storage',
        'Spreading mitigation': 'Manure spreading'
    })

    df_em['category'] = df_em['source'] + ' - ' + df_em['animal']
    mitigations_join['category'] =\
        mitigations_join['source'] + ' - ' + mitigations_join['animal']

    df_links = pd.merge(df_em[['category', 'unique_name']],
                        mitigations_join[['category', 'unique_name']],
                        on='category', how='outer')
    df_links = df_links.rename(
        columns={'unique_name_x': 'farm_lodging_type_id',
                 'unique_name_y': 'farm_reductive_lodging_system_id'}
        )
    df_links = df_links.replace(create_farm_ids_dict(df_em)[1])
    df_links = df_links.replace(create_farm_ids_dict(df_mit)[1])

    df_links = df_links[(df_links['farm_lodging_type_id'].notna()) &
                        (df_links['farm_reductive_lodging_system_id'].notna())]

    table = Table()
    table.data = df_links[['farm_lodging_type_id',
                           'farm_reductive_lodging_system_id']]
    table.name = 'farm_lodging_types_to_reductive_lodging_systems'
    return(table)
