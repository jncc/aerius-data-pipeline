import os
import pandas as pd
import numpy as np

import lib_database_create as libdb


#########################################################################
#                        global variables
#########################################################################

# TODO set unit tests on the funcs that use this making sure it doesn't change
housing_cats = ['Housing systems',
                'Grazing emissions',
                'Outdoor yards (collecting/feeding)']

#########################################################################
#                        sub_df_functions
#########################################################################


def _assign_scrub(house):
    if house in housing_cats:
        return('t')
    return('f')


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


def process_ag_emissions_data(path):
    '''Processes the entire ag emissions input data and generates three
    dataframes, one for the emissions factors, one for the mitigation
    factors and one for the overall N excretions. Takes a path to the
    data file as input
    '''
    df_full = pd.read_excel(path)

    # adds a blank line to the bottom to keep the section to blank line
    # logic the same below.
    df_full.loc[df_full.shape[0]] = [np.NaN] * df_full.shape[1]

    #########################################################################
    #                        emission factors

    # df_housing_em = _create_housing_sub_df(df_full, "Housing systems")
    df_housing_em = _create_sub_df(df_full, "Housing systems")
    df_grazing_em = _create_sub_df(df_full, "Grazing emissions")
    df_yard_em = _create_sub_df(df_full, "Outdoor yards (collecting/feeding)")
    df_storage_em = _create_sub_df(df_full, "Manure storage")
    df_spreading_em = _create_sub_df(df_full, "Manure spreading")

    df_em = pd.concat([df_housing_em, df_grazing_em, df_yard_em, df_storage_em,
                       df_spreading_em], axis=0)

    #########################################################################
    #                        mitigation factors

    df_housing_mit = _create_sub_df(df_full, "Housing mitigation")
    df_yard_mit = _create_sub_df(df_full, "Yard mitigation")
    df_storage_mit = _create_sub_df(df_full, "Storage mitigation")
    df_spreading_mit = _create_sub_df(df_full, "Spreading mitigation")

    df_mit = pd.concat([df_housing_mit, df_yard_mit, df_storage_mit,
                        df_spreading_mit], axis=0)

    df_mit = df_mit.rename(columns={'emmision_factor': 'reduction_factor'})

    return(df_em, df_mit)


def set_unique_farm_name(df):
    return(df['farm_type'] + ' - (' + df['animal'] + '/' + df['source'] + ')')


def create_farm_ids_dict(df_id):

    # the code is used to create the id dictionary later
    # needs name and animal as some names are duplicated
    df_id['unique_name'] = set_unique_farm_name(df_id)
    df_id = libdb._create_id_col(df_id, 'farm_id')

    id_to_val = dict(zip(df_id['farm_id'], df_id['unique_name']))
    val_to_id = {value: key for (key, value) in id_to_val.items()}

    return(id_to_val, val_to_id)

#########################################################################
#                        create table functions
#########################################################################


def create_table_farm_animal_categories(df):

    df_animal = pd.DataFrame({
        'name': df['animal'].unique()
    })

    df_animal = libdb._create_id_col(df_animal, 'farm_animal_category_id')
    df_animal['code'] = df_animal['farm_animal_category_id']
    df_animal['description'] = ' '

    return(df_animal[['farm_animal_category_id', 'code', 'name',
                      'description']])


def create_farm_lodging_types(df, animal_names):
    '''creates the farm lodging system table from the emission
    dataframe. uses the animal names dictionary as a second argument.
    returns a dataframe
    '''

    df_new = pd.DataFrame({
        'farm_animal_category_id': df['animal'],
        'name': df['farm_type'],
        'description': df['source']
    })

    # the code is used to create the id dictionary later
    # needs name and animal as some names are duplicated
    df_new['name'] = set_unique_farm_name(df)
    # df_new['scrubber'] = 'f'
    df_new['scrubber'] = df_new.apply(
        lambda df_new: _assign_scrub(df_new['description']), axis=1
        )

    # replaceing the animal names with ids and using the dicitonary
    # to keep things consistent
    for name, id in animal_names.items():
        df_new['farm_animal_category_id'] =\
                            df_new['farm_animal_category_id'].replace(name, id)
    # Creating a loding system id
    df_new = libdb._create_id_col(df_new, 'farm_lodging_type_id')
    df_new['code'] = df_new['farm_lodging_type_id']

    return(df_new[['farm_lodging_type_id', 'farm_animal_category_id', 'code',
                   'name', 'description', 'scrubber']])


def create_farm_reductive_lodging_system(df, animal_names):
    '''creates the farm reductive lodging system tables. requires the
    mitigation total dataframe and the animal dictionary as inputs.
    retunrs a dataframe
    '''

    df_new = pd.DataFrame({
        'farm_animal_category_id': df['animal'],
        'name': df['farm_type'],
        'description': df['source']
    })

    # the code is used to create the id dictionary later
    # needs name and animal as some names are duplicated
    df_new['name'] = set_unique_farm_name(df)
    # df_new['scrubber'] = 'f'
    df_new['scrubber'] = df_new.apply(
        lambda df_new: _assign_scrub(df_new['description']), axis=1
        )

    # replaceing the animal names with ids and using the dicitonary
    # to keep things consistent
    for name, id in animal_names.items():
        df_new['farm_animal_category_id'] =\
                        df_new['farm_animal_category_id'].replace(name, id)
    # creating a mitigation system id
    df_new = libdb._create_id_col(df_new, 'farm_reductive_lodging_system_id')
    df_new['code'] = df_new['farm_reductive_lodging_system_id']

    return(df_new[[
        'farm_reductive_lodging_system_id', 'farm_animal_category_id', 'code',
        'name', 'description', 'scrubber'
        ]])


def create_table_farm_lodging_type_emission_factors(df):

    # getting the ids in the same way for each table
    # df_em_total['unique_name'] = set_unique_farm_name(df)
    df['farm_lodging_type_id'] = set_unique_farm_name(df)
    df = df.replace(create_farm_ids_dict(df_em_total)[1])

    # converting from kg to g
    df['emmision_factor'] = df['emmision_factor'] * 1000

    df['substance_id'] = libdb.get_substance_id('nh3')
    return(df[['farm_lodging_type_id', 'substance_id', 'emmision_factor']])


def create_table_farm_reductive_lodging_system_reduction_factor(df):

    df_mit_total['farm_reductive_lodging_system_id'] = set_unique_farm_name(df)
    # df['farm_reductive_lodging_system_id'] = \
    #     df['farm_type'] + ' - ' + df['animal']
    df = df.replace(create_farm_ids_dict(df_mit_total)[1])

    df['reduction_factor'] = df['reduction_factor'] / 100

    df['substance_id'] = libdb.get_substance_id('nh3')
    return(df[[
        'farm_reductive_lodging_system_id', 'substance_id', 'reduction_factor'
        ]])


def create_table_farm_lodging_types_to_reductive_lodging_systems(
        df_em, df_mit
        ):

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

    return(df_links[[
        'farm_lodging_type_id', 'farm_reductive_lodging_system_id'
        ]])


#########################################################################
#                             script
#########################################################################

if __name__ == "__main__":

    output_c = './output/aerius_data_22-02-23v2/'
    example_path = './data/ag_emissions/all/'
    file_list = os.listdir(example_path)

    emissions_list = []
    mitigations_list = []
    for file in file_list:
        print(file)
        df_em, df_mit = process_ag_emissions_data(example_path+file)
        emissions_list.append(df_em)
        mitigations_list.append(df_mit)

    df_em_total = pd.concat(emissions_list)
    df_mit_total = pd.concat(mitigations_list)

    # farm_links = create_table_farm_lodging_types_to_reductive_lodging_systems(df_em_total, df_mit_total)
    # farm_links.to_csv(output_c+'farm_lodging_types_to_reductive_lodging_systems.txt', index=False, sep='\t')

    # df_animal = create_table_farm_animal_categories(df_em_total)
    # animal_dict = dict(zip(df_animal['name'], df_animal['farm_animal_category_id']))
    # df_animal.to_csv(output_c+'farm_animal_categories.txt', index=False, sep='\t')

    # df_lodging_types = create_farm_lodging_types(df_em_total, animal_dict)
    # lodging_type_dict = dict(zip(df_lodging_types['code'], df_lodging_types['farm_lodging_type_id']))
    # df_lodging_types.to_csv(output_c+'farm_lodging_types.txt', index=False, sep='\t')

    # df_mitigation_types = create_farm_reductive_lodging_system(df_mit_total, animal_dict)
    # reduction_type_dict = dict(zip(df_mitigation_types['code'], df_mitigation_types['farm_reductive_lodging_system_id']))
    # df_mitigation_types.to_csv(output_c+'farm_reductive_lodging_systems.txt', index=False, sep='\t')

    # lodging_emissions = create_table_farm_lodging_type_emission_factors(df_em_total, lodging_type_dict)
    # lodging_emissions.to_csv(output_c+'farm_lodging_type_emission_factors.txt', index=False, sep='\t')

    lodging_reductions = create_table_farm_reductive_lodging_system_reduction_factor(df_mit_total)
    lodging_reductions.to_csv(output_c+'farm_reductive_lodging_system_reduction_factors.txt', index=False, sep='\t')

    # substances = libdb.create_table_substances()
    # substances.to_csv(output_c+'substances.txt', index=False, sep='\t')
