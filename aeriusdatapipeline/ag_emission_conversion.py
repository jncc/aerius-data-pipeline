# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 10:09:08 2022

@author: Ollie.Grint
"""
# Load up the required packages
import os
import sys
import time
import pandas as pd
import numpy as np
import csv
pd.options.mode.chained_assignment = None
from pathlib import Path

from .lib_database_create import get_substance_id
from .export import Table



##################################################################################################################

# Create a dictionary that will be used to convert the units to wordings required for the tool
units_dict = {
 'kg NH3/animal place/day':'per_animal_per_day',
 'kg NH3/animal place/year':'per_animal_per_year',
 'kg NH3/m2/year':'per_meters_squared_per_year',
 'kg NH3/m3/spray':'per_meters_cubed_per_application',
 'kg NH3/t/spray':'per_tonnes_per_application',
 'kg NH3/m2/day':'per_meters_squared_per_day'
 }

# Create a dictionary for the codes that each animal has been assigned
Animal_catagories_code = {
    'Cattle':'A',
    'Dairy cows':'A1',
    'Dairy replacements (>1 year)':'A1.1',
    'Dairy heifers':'A1.2',
    'Dairy calves (<1 year)':'A1.3',
    'Beef cows':'A2',
    'Beef cattle (>1 year)':'A2.1',
    'Beef heifers in calf':'A2.2',
    'Beef calves (<1 year)':'A2.3',
    'Pigs':'D',
    'Sows':'D1',
    'Farrowers':'D2',
    'Weaners':'D3',
    'Growers':'D4',
    'Finishers':'D5',
    'Boars':'D6',
    'Poultry':'E',
    'Layers':'E1',
	'Barn and free range':'E2',
    'Broilers':'E3',
    'Pullets':'E4',
    'Ducks':'G1',
    'Turkeys (female)':'F1',
    'Turkeys (male)':'F2'
}

# Create a dictionary which provides each animal group with a master code
master_animal_dict_code={
    'Cattle':'A',
    'Poultry':'E',
    'Pigs':'D'
    }

# Create a dictionary which maps each animal type to their master animal type
master_dict={
    'Cattle':'Cattle',
    'Dairy cows':'Cattle',
    'Dairy replacements (>1 year)':'Cattle',
    'Dairy heifers':'Cattle',
    'Dairy calves (<1 year)':'Cattle',
    'Beef cows':'Cattle',
    'Beef cattle (>1 year)':'Cattle',
    'Beef heifers in calf':'Cattle',
    'Beef calves (<1 year)':'Cattle',
    'Pigs':'Pigs',
    'Sows':'Pigs',
    'Farrowers':'Pigs',
    'Weaners':'Pigs',
    'Growers':'Pigs',
    'Finishers':'Pigs',
    'Boars':'Pigs',
    'Poultry':'Poultry',
    'Layers':'Poultry',
	'Barn and free range':'Poultry',
    'Broilers':'Poultry',
    'Pullets':'Poultry',
    'Ducks':'Poultry',
    'Turkeys (female)':'Poultry',
    'Turkeys (male)':'Poultry'
    }


##################################################################################################################

# Define a function which inserts a space into a string (after the first character)
def insert_space(string):
    '''inserts a space in the code to give the name'''
    return string[0:1] + ' ' + string[1:]

##################################################################################################################

def ag_units_recalculate(df):
    '''Recalculates the units into appropriate form - splits the dataframe into component parts depending on units so 
    these can be renamed and recalculated and then recombines them'''

    # To standardise the units, split out into component parts
    kg_NH3_AP_Y = df.loc[df['NH3_UNITS']=='kg NH3/animal place/year']
    kg_NH3_AP_grazingday = df.loc[df['NH3_UNITS']=='kg NH3/animal/grazing day']
    kg_NH3_AP_housedday = df.loc[df['NH3_UNITS']=='kg NH3/animal/housed day']
    kg_NH3_m2 = df.loc[df['NH3_UNITS']=='kg NH3/m2']
    kg_NH3_m2_d = df.loc[df['NH3_UNITS']=='kg NH3/m2/d']
    kg_NH3_m3_slurry = df.loc[df['NH3_UNITS']=='kg NH3/m3 slurry spread']
    kg_NH3_t = df.loc[df['NH3_UNITS']=='kg NH3/t']
    kg_NH3_t_slurryspread = df.loc[df['NH3_UNITS']=='kg NH3/t slurry spread']
    kg_NH3_tonne_fresh = df.loc[df['NH3_UNITS']=='kg NH3/tonne fresh manure']
    
    # recalculate the units and redefine the unit wording
    kg_NH3_AP_grazingday['NH3_UNITS']='kg NH3/animal place/day'
    kg_NH3_AP_housedday['NH3_UNITS']='kg NH3/animal place/day'
    kg_NH3_m2['NH3_UNITS']='kg NH3/m2/day'
    kg_NH3_m2_d['NH3_UNITS']='kg NH3/m2/day'
    kg_NH3_m3_slurry['NH3_UNITS']='kg NH3/m3/spray'
    kg_NH3_t['NH3_UNITS']='kg NH3/t/spray'
    kg_NH3_t_slurryspread['NH3_UNITS']='kg NH3/t/spray'
    kg_NH3_tonne_fresh['NH3_UNITS']='kg NH3/t/spray'
    
    # Recombine to a master dataset with fixed units
    Agricultural_emmissions_fixed_units = kg_NH3_AP_Y
    Agricultural_emmissions_fixed_units = pd.concat([Agricultural_emmissions_fixed_units,kg_NH3_AP_grazingday], axis=0)
    Agricultural_emmissions_fixed_units = pd.concat([Agricultural_emmissions_fixed_units,kg_NH3_AP_housedday], axis=0)
    Agricultural_emmissions_fixed_units = pd.concat([Agricultural_emmissions_fixed_units,kg_NH3_m2], axis=0)
    Agricultural_emmissions_fixed_units = pd.concat([Agricultural_emmissions_fixed_units,kg_NH3_m2_d], axis=0)
    Agricultural_emmissions_fixed_units = pd.concat([Agricultural_emmissions_fixed_units,kg_NH3_m3_slurry], axis=0)
    Agricultural_emmissions_fixed_units = pd.concat([Agricultural_emmissions_fixed_units,kg_NH3_t], axis=0)
    Agricultural_emmissions_fixed_units = pd.concat([Agricultural_emmissions_fixed_units,kg_NH3_t_slurryspread], axis=0)
    Agricultural_emmissions_fixed_units = pd.concat([Agricultural_emmissions_fixed_units,kg_NH3_tonne_fresh], axis=0)
    return(Agricultural_emmissions_fixed_units)

##################################################################################################################

def split_system_types(df,system_type):
    '''split out each of the system types from the dataset, these will then be processed individually'''
    if system_type=='Housing':
        # Split out housing
        Housing = df[df['SYSTEM_TYPE']=='Housing']
        # Remove yard as this is not treated as a housing type
        Housing = Housing[~Housing['LIVESTOCK'].str.contains(" yard ")]
        # Reset the index
        Housing.index = np.arange(1, len(Housing) + 1)
        # Use the index to define a farm lodging type ID
        Housing['farm_lodging_type_id']=Housing.index
        return(Housing)
    else:
        # subset everything that isn't housing
        Agricultural_emmissions_fixed_units_no_housing=df[df['SYSTEM_TYPE']!='Housing']
        # SUbset out yard (this is called housing in the data but isn't treated as housing in Aerius)
        yard=df[df['LIVESTOCK'].str.contains(" yard ")]
        # Define Yard as a system type
        yard['SYSTEM_TYPE']='Yard'
        # Combine yard into the no housind dataframe
        Agricultural_emmissions_fixed_units_no_housing=pd.concat([Agricultural_emmissions_fixed_units_no_housing,yard], axis=0)
        # reset the index
        Agricultural_emmissions_fixed_units_no_housing.index = np.arange(1, len(Agricultural_emmissions_fixed_units_no_housing) + 1)
        # Use the index to define a farm source category ID
        Agricultural_emmissions_fixed_units_no_housing['farm_source_category_id']=Agricultural_emmissions_fixed_units_no_housing.index
        # split this dataset into its component parts depending on system type
        system_type_temp = Agricultural_emmissions_fixed_units_no_housing[Agricultural_emmissions_fixed_units_no_housing['SYSTEM_TYPE']==system_type]
        # reset the new dataframes index
        system_type_temp.index = np.arange(1, len(system_type_temp) + 1)
        return(system_type_temp)


##################################################################################################################
# Process the analysis ready for the farm animal category table
def farm_animals_prep(df):
    '''process the housing data to define out the farm animal categories'''
    # Convert the pre-defined dictionary into a dataframe
    Farm_animal_catagories_processing=pd.DataFrame.from_dict(Animal_catagories_code,orient='index')
    # Fix the column names
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing.index
    Farm_animal_catagories_processing.columns = ['code','description']
    # Rename the yard descriptions to the animal types that they are
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['description'].map(lambda x: x.replace('Beef - yard no cleaning','Beef cows'))
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['description'].map(lambda x: x.replace('Beef - yard scraped daily','Beef cows'))
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['description'].map(lambda x: x.replace('Beef - yard washed daily','Beef cows'))
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['description'].map(lambda x: x.replace('Dairy - yard no cleaning','Dairy cows'))
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['description'].map(lambda x: x.replace('Dairy - yard scraped daily','Dairy cows'))
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['description'].map(lambda x: x.replace('Dairy - yard washed daily','Dairy cows'))
    # Remove any duplicates from this table
    Farm_animal_catagories_processing = Farm_animal_catagories_processing.drop_duplicates()
    # Use the master dictionary to include the parent animal type in this dataset
    Farm_animal_catagories_processing['animal']=Farm_animal_catagories_processing['description'].map(master_dict)
    # Combine the parent animal with the description 
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['animal']+ ' - '+Farm_animal_catagories_processing['description']
    # Remove any instances where the parent animal was in the description and so is now duplicated (such as Poultry - Poultry)
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['description'].map(lambda x: x.replace('Cattle - Cattle','Cattle'))
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['description'].map(lambda x: x.replace('Poultry - Poultry','Poultry'))
    Farm_animal_catagories_processing['description']=Farm_animal_catagories_processing['description'].map(lambda x: x.replace('Pigs - Pigs','Pigs'))
    # Reset the index
    Farm_animal_catagories_processing.index = np.arange(1, len(Farm_animal_catagories_processing) + 1)
    # Use the index to set farm animal category id
    Farm_animal_catagories_processing['farm_animal_category_id']=Farm_animal_catagories_processing.index
    # Use the insert space function to insert a space into the code
    Farm_animal_catagories_processing['name'] = Farm_animal_catagories_processing.apply(
        lambda Farm_animal_catagories_processing: insert_space(Farm_animal_catagories_processing['code']), axis=1)
    # Returns what will be the farm animal categories table as a dataframe
    return(Farm_animal_catagories_processing)

# convert the farm animal categories dataframe to a dictionary to be used to give the farm_animal_category_id in future
def convert_pandas_to_dict(df):    
    ''' Converts a dataframe to a dictionary'''
    Farm_animal_catagories_dict = dict(zip(df['description'],df['farm_animal_category_id'].astype(int)))
    return(Farm_animal_catagories_dict)


##################################################################################################################

    # Function to load the NI filepath
def load_NI_emissions(filepath, df):
    '''This loads the northern ireland emissions file and gives it a farm_lodging_type_id that matches the existing farm_lodging type ids'''
    # read the data
    NI_data = pd.read_csv(filepath)
    # calculate the max value of the existing farm lodging type id
    max_val=max(df['farm_lodging_type_id'])
    # use this to start from the previous max value and add new id's for the NI data
    NI_data['farm_lodging_type_id'] = np.arange(max_val+1, len(NI_data) + max_val+1)
    return(NI_data)

# function to merge the NI data 
def combine_Ni_data(df,df_ni):
    '''Merge the NI data with the existing data'''
    # set values for whether the data originated from NI or not
    df['origin']=''
    df_ni['origin']='.NI'
    # Merge the datasets
    df_merge=pd.concat([df,df_ni], axis=0)
    return(df_merge)

##################################################################################################################


def grazing_code_creation(dict1):
    '''This will create a unique code for grazing, where the code is the parent animal, then itterated by decription.
    This takes the input as a dictionary of the start of the code (eg. A.Grazing) and the ID'''
    # Set counts for each animal type
    count_a = 1
    count_e = 1
    count_d = 1
    # create an empty dictionary that will be added to
    dict2={}
    # Loop through the dictionary and for each animal type increase the count and add it to the new dictionary
    for k,v in dict1.items():
        if v[:1]=='A':
            code=v+'.'+str(count_a)
            count_a=count_a+1
            dict2[k]=code
        if v[:1]=='E':
            code=v+'.'+str(count_e)
            count_e=count_e+1
            dict2[k]=code
        if v[:1]=='D':
            code=v+'.'+str(count_d)
            count_d=count_d+1
            dict2[k]=code
    # Return a dictionary of the new codes
    return(dict2)
            
##################################################################################################################


def process_grazing(df,dict_ag):
    '''prep the data for the grazing sector, takes grazing df as an input'''
    # copy the grazing df to be edited
    sector_processing = df.copy()
    # remove grazing from the dataframe as this will be provided by the sector ID
    sector_processing['LIVESTOCK'] = sector_processing['LIVESTOCK'].map(lambda x: x.replace(' grazing',''))
    # Provide the hardcoded sector ID
    sector_processing['Sector_ID'] = 4130    
    # create a temporary column that will be used to map the farm_animal_category_id - need parent animal and animal
    sector_processing['temp_for_dict_lookup']=sector_processing['ANIMAL_TYPE']+ ' - '+sector_processing['LIVESTOCK']
    # map the farm_animal_catgeory_id
    sector_processing['farm_animal_category_id']=sector_processing['temp_for_dict_lookup'].map(dict_ag)
    # For the name replace blanks in the description with _
    sector_processing['name'] = sector_processing['LIVESTOCK'].map(lambda x: x.replace(" ", "_"))
    # reorder column
    sector_processing['description'] = sector_processing['LIVESTOCK']
    # convert the units with the earlier made dictionary
    sector_processing['farm_emission_factor_type'] = sector_processing['NH3_UNITS'].map(units_dict)
    # for part one of the code get the parent animal code
    sector_processing['code_part_1']=sector_processing['ANIMAL_TYPE'].map(master_animal_dict_code)
    # part 2 of the code is grazing
    sector_processing['code_part_2']='Grazing'
    # join code parts
    sector_processing['code_part_1+2']=sector_processing['code_part_1']+'.'+sector_processing['code_part_2']
    # zip to a dictionary ready for the code creation function
    dict1=dict(zip(sector_processing['farm_source_category_id'],sector_processing['code_part_1+2']))
    # run function to add numbers to the code so each animal has 1-max numbers
    farm_source_categories_code_dict=grazing_code_creation(dict1)
    # use the created dictionary to generate the codes
    sector_processing['code']=sector_processing['farm_source_category_id'].map(farm_source_categories_code_dict)
    # subset columns
    sector_processing=sector_processing[['farm_source_category_id','Sector_ID','code','farm_animal_category_id','name','description','farm_emission_factor_type']]
    return(sector_processing)


##################################################################################################################

def process_housing_types_ef(df):
    '''Process the data to create a df in the format of the housing types table'''
    # Make a copy of the database to edit
    sector_processing_EF = df.copy()
    # Set the substance ID
    sector_processing_EF['substance_id']=get_substance_id('nh3')
    # rename the column
    sector_processing_EF['emission_factor']=sector_processing_EF['NH3_EF']
    # Select which columns will be in the table
    sector_processing_EF=sector_processing_EF[['farm_source_category_id','substance_id','emission_factor']]
    return(sector_processing_EF)

    
##################################################################################################################

def process_sector(df,sector_val,keyword, dict_ag):
    '''prep the data for each of the sectors (not housing)'''
    # make a copy of the sector data
    sector_processing = df.copy()
    # input the pre-defined sector value
    sector_processing['Sector_ID'] = sector_val
    # subsetction to process yard data
    if keyword=='Yard':
        # split out pre and post hyphon
        sector_processing[['A', 'B']] = sector_processing['LIVESTOCK'].str.split(' - ', 1, expand=True)
        # pre hypohon is livestock information
        sector_processing['LIVESTOCK']=sector_processing['A']
        # post hyphon is housing system
        sector_processing['HOUSINGSYSTEM']=sector_processing['B']
        # capitalise yard
        sector_processing['HOUSINGSYSTEM']=sector_processing['HOUSINGSYSTEM'].map(lambda x: x.replace('yard','Yard'))
        # rename the livestock to match existing names
        sector_processing['LIVESTOCK']=sector_processing['LIVESTOCK'].map(lambda x: x.replace('Beef','Cattle - Beef cows'))
        sector_processing['LIVESTOCK']=sector_processing['LIVESTOCK'].map(lambda x: x.replace('Dairy','Cattle - Dairy cows'))
        # map the farm animal id for yard
        sector_processing['farm_animal_category_id']=sector_processing['LIVESTOCK'].map(dict_ag)
    else:
        # map the farm animal id for not yard
        sector_processing['farm_animal_category_id']=sector_processing['ANIMAL_TYPE'].map(dict_ag)
    # replace na with empty strings
    sector_processing=sector_processing.fillna('')
    # create the description by adding together other column information
    sector_processing['description'] = sector_processing['LIVESTOCK']+' ('+sector_processing['ANIMAL_TYPE']+' - '+sector_processing['HOUSINGSYSTEM']+')'
    # if no information in the bracket, remove the bracket, similar for after hyphon
    sector_processing['description']=sector_processing['description'].map(lambda x: x.replace(' ()',''))
    sector_processing['description']=sector_processing['description'].map(lambda x: x.replace(' - )',')'))
    # for name replace gap with _
    sector_processing['name'] = sector_processing['description'].map(lambda x: x.replace(" ", "_"))
    # map the units with the premade units dict
    sector_processing['farm_emission_factor_type'] = sector_processing['NH3_UNITS'].map(units_dict)
    # use the parent animls code
    sector_processing['code_part_1']=sector_processing['ANIMAL_TYPE'].map(master_animal_dict_code)
    # create the empty dataframe
    sector_df=pd.DataFrame()
    
    # subset parent animals with code a
    sector_a=sector_processing[sector_processing['code_part_1']=='A']
    # subset only livestock column
    sector_a=sector_a[['LIVESTOCK']]
    # drop duplicates
    sector_a=sector_a.drop_duplicates()
    # re-index the dataframe
    sector_a.index = np.arange(1, len(sector_a) + 1)
    # use the index to create the value, this will be used to make sure for each animal the count starts from 1
    sector_a['value']=sector_a.index
    # add to the premade df
    sector_df=pd.concat([sector_df,sector_a], axis=0) 
    # subset parent animals with code e
    sector_e=sector_processing[sector_processing['code_part_1']=='E']
    # subset only livestock column
    sector_e=sector_e[['LIVESTOCK']]
    # drop duplicates
    sector_e=sector_e.drop_duplicates()
    # re-index the dataframe
    sector_e.index = np.arange(1, len(sector_e) + 1)
    # use the index to create the value, this will be used to make sure for each animal the count starts from 1
    sector_e['value']=sector_e.index
    # add to the premade df
    sector_df=pd.concat([sector_df,sector_e], axis=0) 
    # subset parent animals with code d
    sector_d=sector_processing[sector_processing['code_part_1']=='D']
    # subset only livestock column
    sector_d=sector_d[['LIVESTOCK']]
    # drop duplicates
    sector_d=sector_d.drop_duplicates()
    # re-index the dataframe
    sector_d.index = np.arange(1, len(sector_d) + 1)
    # use the index to create the value, this will be used to make sure for each animal the count starts from 1
    sector_d['value']=sector_d.index
    # add to the premade df
    sector_df=pd.concat([sector_df,sector_d], axis=0) 
    # use this dataframe of defined values to make a dictionary to make the code - each section of the 
    # livestock now has a new code
    sector_dict=dict(zip(sector_df['LIVESTOCK'],sector_df['value']))
    # map this to create an element of the code
    sector_processing['code_part']=sector_processing['LIVESTOCK'].map(sector_dict)
    # use the pre-defined keyword to add into code
    sector_processing['code_part_2']=keyword
    # subset the data needed to make the code
    df=sector_processing[['farm_source_category_id','code_part','code_part_1','code_part_2']]  
    # pull out the parent animal code
    system=df['code_part_2'].iloc[0]
    # create an empty dataframe
    data_1=pd.DataFrame()
    # if there are each animal type, loop through
    if (df['code_part_1']=='A').sum()>0:
        # subset each animal type
        temp_a=df[df['code_part_1']=='A']
        # calculate the range of values that exist for livestock codes 
        min_val_a=min(temp_a['code_part'])
        max_val_a=max(temp_a['code_part'])
        # loop through each values that exist for livestock codes 
        for i in range(min_val_a,max_val_a+1):
            # subset each values that exist for livestock codes 
            tempdf=temp_a[temp_a['code_part']==i]
            # reset the index and use it to give a new value to be added to the code
            tempdf.index = np.arange(1, len(tempdf) + 1)
            tempdf['value']=tempdf.index
            val=str(i)
            # add code together 
            tempdf['code']='A.'+system+'.'+val+'.'+tempdf['value'].astype(str)
            # add the new dataframe
            data_1=pd.concat([data_1,tempdf], axis=0)
    # if there are each animal type, loop through            
    if (df['code_part_1']=='E').sum()>0:
        # subset each animal type
        temp_e=df[df['code_part_1']=='E']
        # calculate the range of values that exist for livestock codes 
        min_val_e=min(temp_e['code_part'])
        max_val_e=max(temp_e['code_part'])
        # loop through each values that exist for livestock codes 
        for i in range(min_val_e,max_val_e+1):
            # subset each values that exist for livestock codes 
            tempdf=temp_e[temp_e['code_part']==i]
            # reset the index and use it to give a new value to be added to the code
            tempdf.index = np.arange(1, len(tempdf) + 1)
            tempdf['value']=tempdf.index
            val=str(i)
            # add code together 
            tempdf['code']='E.'+system+'.'+val+'.'+tempdf['value'].astype(str)
            # add the new dataframe
            data_1=pd.concat([data_1,tempdf], axis=0)
# if there are each animal type, loop through
    if (df['code_part_1']=='D').sum()>0:
        # subset each animal type
        temp_d=df[df['code_part_1']=='D']
        # calculate the range of values that exist for livestock codes 
        min_val_d=min(temp_d['code_part'])
        max_val_d=max(temp_d['code_part'])
        # loop through each values that exist for livestock codes 
        for i in range(min_val_d,max_val_d+1):
            # subset each values that exist for livestock codes 
            tempdf=temp_d[temp_d['code_part']==i]
            # reset the index and use it to give a new value to be added to the code
            tempdf.index = np.arange(1, len(tempdf) + 1)
            tempdf['value']=tempdf.index
            val=str(i)
            # add code together 
            tempdf['code']='D.'+system+'.'+val+'.'+tempdf['value'].astype(str)
            # add the new dataframe
            data_1=pd.concat([data_1,tempdf], axis=0)    
    # use the dataframe to make a dictionary to give a code
    dict1=dict(zip(data_1['farm_source_category_id'],data_1['code']))
    # use the dictionary to give each entry a unique code
    sector_processing['code']=sector_processing['farm_source_category_id'].map(dict1)  
    # select the columns to be used 
    sector_processing=sector_processing[['farm_source_category_id','Sector_ID','code','farm_animal_category_id','name','description','farm_emission_factor_type']]
    return(sector_processing)

##################################################################################################################

def manure_storage_fix_units(df):
    '''fix the units around a small subset of columns - units should be differet if in or not in pigs'''
    # subset pigs and not pigs
    pigs = df[df['farm_animal_category_id']==10]
    no_pigs=df[df['farm_animal_category_id']!=10]
    # make a dictionary for fixing the units for pigs
    fixing_units_dict_pigs={
        'per_tonnes_per_application':'per_tonnes_per_year',
        'per_meters_squared_per_day':'per_meters_squared_per_year'}
    # make a dictionary for fixing the units for no pigs
    fixing_units_dict_no_pigs={
        'per_tonnes_per_application':'per_tonnes_per_year',
        'per_meters_squared_per_day':'per_meters_squared_per_day'}
    # use the dictionaries to fix the units
    pigs['farm_emission_factor_type']=pigs['farm_emission_factor_type'].map(fixing_units_dict_pigs)
    no_pigs['farm_emission_factor_type']=no_pigs['farm_emission_factor_type'].map(fixing_units_dict_no_pigs)
    # join the dataframes back together 
    df2=pd.concat([pigs,no_pigs], axis=0)
    return(df2)
    
##################################################################################################################

def create_farm_animal_categories_table(df):
    '''subset only the rows needed for the table'''
    farm_animal_catagories=df[['farm_animal_category_id','code','name','description']]
    return(farm_animal_catagories)

##################################################################################################################
# Farm lodging types

def create_farm_lodging_types_table(df,dict_ag):
    '''create the farm lodging types table from the housing data'''
    farm_lodging_types_processing = df
    # add the parent animal to the animal to be searched using the dictionary
    farm_lodging_types_processing['temp_for_dict_lookup']=farm_lodging_types_processing['ANIMAL_TYPE']+ ' - '+farm_lodging_types_processing['LIVESTOCK']
    # use the dictionary to make the farm_animal_catgory id
    farm_lodging_types_processing['farm_animal_category_id']=farm_lodging_types_processing['temp_for_dict_lookup'].map(dict_ag)
    # use a dictionary to add in the code
    farm_lodging_types_processing['code']=farm_lodging_types_processing['LIVESTOCK'].map(Animal_catagories_code)
    g = farm_lodging_types_processing.groupby('code')
    # adds the counting number suffix
    farm_lodging_types_processing['code'] += g.cumcount().add(1).astype(str).radd('.').\
        mask(g['code'].transform('count') == 1, '')
    farm_lodging_types_processing['code']=farm_lodging_types_processing['code'].astype(str) 
    # add the information for whether the data is from NI or not       
    farm_lodging_types_processing['code']=farm_lodging_types_processing['code']+farm_lodging_types_processing['origin']
    # inset a space into the code for the name
    farm_lodging_types_processing['name'] = farm_lodging_types_processing.apply(
        lambda farm_lodging_types_processing: insert_space(farm_lodging_types_processing['code']), axis=1
        )
    # make the description by adding other columns together
    farm_lodging_types_processing['description']=farm_lodging_types_processing['HOUSINGSYSTEM'] +' (' + farm_lodging_types_processing['ANIMAL_TYPE']+' - '+farm_lodging_types_processing['LIVESTOCK'] + ')'
    # set scrubber as false
    farm_lodging_types_processing['scrubber']='false'
    # rename the units
    farm_lodging_types_processing['farm_emission_factor_type']=farm_lodging_types_processing['NH3_UNITS'].map(units_dict)
    farm_lodging_types_processing['farm_animal_category_id']=farm_lodging_types_processing['farm_animal_category_id'].astype(int)
    # subset the columns
    farm_lodging_types=farm_lodging_types_processing[['farm_lodging_type_id','farm_animal_category_id','code','name','description','scrubber','farm_emission_factor_type']]
    return(farm_lodging_types)



##################################################################################################################
#farm_lodging_type_emission_factor
def create_farm_lodging_type_emission_factor_table(df):
    '''create the farm lodging type emission factor table'''
    farm_lodging_type_emission_factor_processing = df
    # set the substance id
    farm_lodging_type_emission_factor_processing['substance_id']=get_substance_id('nh3')
    # reword the units
    farm_lodging_type_emission_factor_processing['emission_factor']=farm_lodging_type_emission_factor_processing['NH3_EF']
    # subset the columns
    farm_lodging_type_emission_factor=farm_lodging_type_emission_factor_processing[['farm_lodging_type_id','substance_id','emission_factor']]
    return(farm_lodging_type_emission_factor)





##################################################################################################################

def create_farm_source_categories_table(df1,df2,df3,df4):
    ''' group the sectors together to make the farm source categories table '''
    farm_source_categories=df1.copy()
    # group tables together
    farm_source_categories = pd.concat([farm_source_categories,df2], axis=0)
    farm_source_categories = pd.concat([farm_source_categories,df3], axis=0)
    farm_source_categories = pd.concat([farm_source_categories,df4], axis=0)
    return(farm_source_categories)

def create_farm_source_emission_factors_table(df1,df2,df3,df4):
    ''' group the sectors together to make the farm source emission factor table '''
    farm_source_emission_factors=df1.copy()
    # group tables together
    farm_source_emission_factors = pd.concat([farm_source_emission_factors,df2], axis=0)
    farm_source_emission_factors = pd.concat([farm_source_emission_factors,df3], axis=0)
    farm_source_emission_factors = pd.concat([farm_source_emission_factors,df4], axis=0)
    return(farm_source_emission_factors)



##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################

def create_farm_animal_categories():

    # Emmissions filepath
    Agricultural_emmissions_file = 'data\\ag_emissions\\emissions_scail_fixed.csv'

   
    # read the ag emissions file
    Agricultural_emmissions = pd.read_csv(Agricultural_emmissions_file)
    # fix the agricultural emissions file
    Agricultural_emmissions_fixed_units=ag_units_recalculate(Agricultural_emmissions)
    # split out the systems  
    Housing=split_system_types(Agricultural_emmissions_fixed_units,'Housing')
    Grazing=split_system_types(Agricultural_emmissions_fixed_units,'Grazing')
    Manure_storage=split_system_types(Agricultural_emmissions_fixed_units,'Manure storage')
    Manure_application=split_system_types(Agricultural_emmissions_fixed_units,'Manure application')
    Yard=split_system_types(Agricultural_emmissions_fixed_units,'Yard')

    # prepare the farm animals category table    
    Farm_animal_catagories_processing=farm_animals_prep(Housing)
    # convert dataframe to dictionary
    Farm_animal_catagories_dict=convert_pandas_to_dict(Farm_animal_catagories_processing)

        # process the grazing table
    Grazing_processed=process_grazing(Grazing, Farm_animal_catagories_dict)
    # process the emission factor table for grazing
    Grazing_processed_ef=process_housing_types_ef(Grazing)

        # process the data for each sector and the emission factor tables
    Manure_application_processed=process_sector(Manure_application,4140,'Spread',Farm_animal_catagories_dict)
    Manure_application_processed_ef=process_housing_types_ef(Manure_application)
    Manure_storage_processed=process_sector(Manure_storage,4120,'Store',Farm_animal_catagories_dict)
    Manure_storage_processed_ef=process_housing_types_ef(Manure_storage)
    Manure_storage_processed=manure_storage_fix_units(Manure_storage_processed)
    Yard_processed=process_sector(Yard,4160,'Yard',Farm_animal_catagories_dict)
    Yard_processed_ef=process_housing_types_ef(Yard)

    # create the farm animal catgories table and save
    farm_animal_categories=create_farm_animal_categories_table(Farm_animal_catagories_processing)

    table = Table()
    table.data = farm_animal_categories
    table.name = 'farm_animal_categories'
    return(table)


def create_farm_lodging_types():

 # NI emissions filepath
    NI_emmissions_file='data\\ag_emissions\\NI emissions.csv'

# Emmissions filepath
    Agricultural_emmissions_file = 'data\\ag_emissions\\emissions_scail_fixed.csv'

    # read the ag emissions file
    Agricultural_emmissions = pd.read_csv(Agricultural_emmissions_file)
    # fix the agricultural emissions file
    Agricultural_emmissions_fixed_units=ag_units_recalculate(Agricultural_emmissions)
    # split out the systems  
    Housing=split_system_types(Agricultural_emmissions_fixed_units,'Housing')

    # prepare the farm animals category table    
    Farm_animal_catagories_processing=farm_animals_prep(Housing)
    # convert dataframe to dictionary
    Farm_animal_catagories_dict=convert_pandas_to_dict(Farm_animal_catagories_processing)

    # load in the ni data
    NI_Housing=load_NI_emissions(NI_emmissions_file,Housing)
    # combine ni data with existing
    Housing_with_NI=combine_Ni_data(Housing,NI_Housing)
    # create the farm lodging types table and save
    farm_lodging_types=create_farm_lodging_types_table(Housing_with_NI,Farm_animal_catagories_dict)

    table = Table()
    table.data = farm_lodging_types
    table.name = 'farm_lodging_types'
    return(table)


def create_farm_lodging_type_emission_factor():

 # NI emissions filepath
    NI_emmissions_file='data\\ag_emissions\\NI emissions.csv'

# Emmissions filepath
    Agricultural_emmissions_file = 'data\\ag_emissions\\emissions_scail_fixed.csv'

   
    # read the ag emissions file
    Agricultural_emmissions = pd.read_csv(Agricultural_emmissions_file)
    # fix the agricultural emissions file
    Agricultural_emmissions_fixed_units=ag_units_recalculate(Agricultural_emmissions)
    # split out the systems  
    Housing=split_system_types(Agricultural_emmissions_fixed_units,'Housing')

    # load in the ni data
    NI_Housing=load_NI_emissions(NI_emmissions_file,Housing)
    # combine ni data with existing
    Housing_with_NI=combine_Ni_data(Housing,NI_Housing)



    # create the farm lodging types emission factor table and save
    farm_lodging_type_emission_factor=create_farm_lodging_type_emission_factor_table(Housing_with_NI)

    table = Table()
    table.data = farm_lodging_type_emission_factor
    table.name = 'farm_lodging_type_emission_factor'
    return(table)


def create_farm_source_categories():


    # Emmissions filepath
    Agricultural_emmissions_file = 'data\\ag_emissions\\emissions_scail_fixed.csv'

   
    # read the ag emissions file
    Agricultural_emmissions = pd.read_csv(Agricultural_emmissions_file)
    # fix the agricultural emissions file
    Agricultural_emmissions_fixed_units=ag_units_recalculate(Agricultural_emmissions)
    # split out the systems  
    Housing=split_system_types(Agricultural_emmissions_fixed_units,'Housing')
    Grazing=split_system_types(Agricultural_emmissions_fixed_units,'Grazing')
    Manure_storage=split_system_types(Agricultural_emmissions_fixed_units,'Manure storage')
    Manure_application=split_system_types(Agricultural_emmissions_fixed_units,'Manure application')
    Yard=split_system_types(Agricultural_emmissions_fixed_units,'Yard')

    # prepare the farm animals category table    
    Farm_animal_catagories_processing=farm_animals_prep(Housing)
    # convert dataframe to dictionary
    Farm_animal_catagories_dict=convert_pandas_to_dict(Farm_animal_catagories_processing)

        # process the grazing table
    Grazing_processed=process_grazing(Grazing, Farm_animal_catagories_dict)
    # process the emission factor table for grazing
    Grazing_processed_ef=process_housing_types_ef(Grazing)

        # process the data for each sector and the emission factor tables
    Manure_application_processed=process_sector(Manure_application,4140,'Spread',Farm_animal_catagories_dict)
    Manure_application_processed_ef=process_housing_types_ef(Manure_application)
    Manure_storage_processed=process_sector(Manure_storage,4120,'Store',Farm_animal_catagories_dict)
    Manure_storage_processed_ef=process_housing_types_ef(Manure_storage)
    Manure_storage_processed=manure_storage_fix_units(Manure_storage_processed)
    Yard_processed=process_sector(Yard,4160,'Yard',Farm_animal_catagories_dict)
    Yard_processed_ef=process_housing_types_ef(Yard)

    # create the farm source categories table and save
    farm_source_categories=create_farm_source_categories_table(Grazing_processed,Manure_application_processed,Manure_storage_processed,Yard_processed)

    table = Table()
    table.data = farm_source_categories
    table.name = 'farm_source_categories'
    return(table)

def create_farm_source_emission_factors():


    # Emmissions filepath
    Agricultural_emmissions_file = 'data\\ag_emissions\\emissions_scail_fixed.csv'

   
    # read the ag emissions file
    Agricultural_emmissions = pd.read_csv(Agricultural_emmissions_file)
    # fix the agricultural emissions file
    Agricultural_emmissions_fixed_units=ag_units_recalculate(Agricultural_emmissions)
    # split out the systems  
    Housing=split_system_types(Agricultural_emmissions_fixed_units,'Housing')
    Grazing=split_system_types(Agricultural_emmissions_fixed_units,'Grazing')
    Manure_storage=split_system_types(Agricultural_emmissions_fixed_units,'Manure storage')
    Manure_application=split_system_types(Agricultural_emmissions_fixed_units,'Manure application')
    Yard=split_system_types(Agricultural_emmissions_fixed_units,'Yard')

    # prepare the farm animals category table    
    Farm_animal_catagories_processing=farm_animals_prep(Housing)
    # convert dataframe to dictionary
    Farm_animal_catagories_dict=convert_pandas_to_dict(Farm_animal_catagories_processing)

        # process the grazing table
    Grazing_processed=process_grazing(Grazing, Farm_animal_catagories_dict)
    # process the emission factor table for grazing
    Grazing_processed_ef=process_housing_types_ef(Grazing)

        # process the data for each sector and the emission factor tables
    Manure_application_processed=process_sector(Manure_application,4140,'Spread',Farm_animal_catagories_dict)
    Manure_application_processed_ef=process_housing_types_ef(Manure_application)
    Manure_storage_processed=process_sector(Manure_storage,4120,'Store',Farm_animal_catagories_dict)
    Manure_storage_processed_ef=process_housing_types_ef(Manure_storage)
    Manure_storage_processed=manure_storage_fix_units(Manure_storage_processed)
    Yard_processed=process_sector(Yard,4160,'Yard',Farm_animal_catagories_dict)
    Yard_processed_ef=process_housing_types_ef(Yard)

    # create the farm source emission factor table and save
    farm_source_emission_factors=create_farm_source_emission_factors_table(Grazing_processed_ef,Manure_application_processed_ef,Manure_storage_processed_ef,Yard_processed_ef)

    table = Table()
    table.data = farm_source_emission_factors
    table.name = 'farm_source_emission_factors'
    return(table)


