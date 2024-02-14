import pandas as pd
import numpy as np
import copy

import aeriusdatapipeline as adp

# assigning a new col as a string gives a warning
# this stops the arning which is not applicable here
pd.options.mode.chained_assignment = None

nox_path = './data/Road-emissions/final/nox.xlsx'
nh3_path = './data/Road-emissions/final/road_emissions_nh3.xlsx'
hdv_path = './data/Road-emissions/final/hdv.xlsx'
diurnal_path = './data/Road-emissions/final/tra0307.xlsx'

def expand_feature(df, name, expansion_list):
    '''the nh3 data doesn't vary by speed or gradient so doesnt have
    that variable. this gives it a row for each combination
    and assigns it the same emissions value
    '''
    var_dfs = []
    for var in expansion_list:
        df_var = copy.copy(df)
        df_var[name] = var
        var_dfs.append(df_var)

    return(pd.concat(var_dfs))


def prep_nh3_data():
    df_nh3 = pd.read_excel(nh3_path)

    df_nh3 = expand_feature(df_nh3, 'maximum_speed',
                            [5, 16, 32, 48, 64, 80, 97, 113, 127, 140])

    # Separating out the hdv stuff so it can be given a gradient
    df_hdv = df_nh3[(df_nh3['HGV'] == 100) | (df_nh3['Bus and Coach'] == 100)]
    df_hdv = expand_feature(df_hdv, 'gradient',
                            [0, 1, 2, 3, 4, 5, 6, -1, -2, -3, -4, -5, -6])

    # the non HDV vehicles dont vary with gradient
    df_s = df_nh3[(df_nh3['HGV'] != 100) & (df_nh3['Bus and Coach'] != 100)]
    df_s = expand_feature(df_s, 'gradient', [0])

    return(pd.concat([df_s, df_hdv]))


def prep_nox_data():
    df_nx = pd.read_excel(nox_path)

    # convert it to /s
    df_nx['emission_factor'] = df_nx['emission_factor'] / 86400
    df_nx = df_nx[(df_nx['HGV'] != 100) & (df_nx['Bus and Coach'] != 100)]
    df_nx = df_nx[df_nx['gradient'] == 0]
    return(df_nx)


def prep_hdv_data():
    df_hdv = pd.read_excel(hdv_path)

    # convert it to /s
    df_hdv['emission_factor'] = df_hdv['emission_factor'].astype(float) / 86400

    # converting gradients to negative if down hill
    df_hdv["Flow Direction"].replace({
        "Up Hill": 1,
        np.NaN: 1,
        'Down Hill': -1}, inplace=True)
    # Down Hill is converted to -1 so when timesd by the gradient
    # it makes down hill gradients negative
    df_hdv['gradient'] = df_hdv['gradient'].astype(int)\
        * df_hdv['Flow Direction']

    return(df_hdv)


def sort_df(df, lst, colName):
    df['order'] = df[colName].apply(lambda x: lst.index(x))
    return df.sort_values(['order']).drop(columns=['order'])


def convert_100_to_vehicle(df):
    # TODO improve this so no ifs. replace 100 with col name + add all
    # cols together
    def get_vehicle_type(df_sub):
        '''converts the 100% to the name of the vehicle in the data'''
        if df_sub['Car'] == 100.0:
            return('Car')
        if df_sub['Taxi (black cab)'] == 100.0:
            return('Taxi (black cab)')
        if df_sub['LGV'] == 100.0:
            return('LGV')
        if df_sub['HGV'] == 100.0:
            return('HGV')
        if df_sub['Bus and Coach'] == 100.0:
            return('Bus and Coach')
        if df_sub['Motorcycle'] == 100.0:
            return('Motorcycle')

    df['vehicle_type'] = df.apply(lambda row: get_vehicle_type(row), axis=1)
    return(df)


def read_road_data():
    '''reads in the manually obtained EFT data from an excel and combines
    into a full df with all the info needed
    '''

    df_nh3 = prep_nh3_data()
    df_nox = prep_nox_data()
    df_hdv = prep_hdv_data()

    # the data is made up of these three parts
    df = pd.concat([df_nh3, df_nox, df_hdv])
    # converting the 100(%) to the name of the vehicle
    df = convert_100_to_vehicle(df)
    # reordering the df to make the most sense in the UI
    df = sort_df(df, ['Car', 'Taxi (black cab)', 'Motorcycle', 'LGV', 'HGV',
                      'Bus and Coach'], 'vehicle_type')
    # some of the ints get converted to float
    df['maximum_speed'] = df['maximum_speed'].astype(int).astype(str)
    df['year'] = df['year'].astype(int).astype(str)
    return(df)


def _make_code(df_code):
    '''takes the frst three letters to make the code and adds a number
    suffix for duplicate codes
    '''
    def _code_start(row):
        '''takes the first three letters to make the code'''
        return(row['name'][0:3])

    df_code['code'] = df_code.apply(lambda df_code: _code_start(df_code),
                                    axis=1)
    g = df_code.groupby('code')
    # adds the counting number suffix
    df_code['code'] += g.cumcount().add(1).astype(str).\
        mask(g['code'].transform('count') == 1, '')
    return(df_code)

########################################################################
#                       converting id functions                        #
########################################################################


def _convert_speed_profiles(df_convert):
    '''making a combination of speed and gradient for the speed
    profile and converting to id
    '''
    # need to have a single column to convert to ID
    df_convert['speed_profile'] = df_convert['maximum_speed'].astype(str)\
        + '-' + df_convert['gradient'].astype(str)
    # Changing the countries to the ids created in the speed profiles table
    df_speed = create_table_road_speed_profiles().data
    df_speed['speed_profile'] = df_speed['maximum_speed'].astype(str)\
        + '-' + df_speed['gradient'].astype(str)
    speed_dic = adp._create_dictionary_from_df(
        df_speed[['speed_profile', 'road_speed_profile_id']]
        )
    df_convert['road_speed_profile_id'] = adp._convert_id(
                                    df_convert['speed_profile'], speed_dic)
    return(df_convert['road_speed_profile_id'])


def _convert_road_type(df_convert):
    '''creating the road type df to convert the road type to id'''

    # Changing the road_types to the ids created in the road type table
    df_types = create_table_road_type_categories().data
    type_dic = adp._create_dictionary_from_df(
        df_types[['name', 'road_type_category_id']]
        )
    df_convert['road_type'] = adp._convert_id(
                                        df_convert['road_type'], type_dic)

    return(df_convert['road_type'])


def _convert_road_area(df_convert):
    '''creating the road area df to convert the road type to id'''

    # Changing the countries to the ids created in the road
    # area table
    df_areas = create_table_road_area_categories().data
    areas_dic = adp._create_dictionary_from_df(
        df_areas[['name', 'road_area_category_id']]
        )
    df_convert['country'] = adp._convert_id(df_convert['country'], areas_dic)
    return(df_convert['country'])


def _convert_vehicle_type(df_convert):
    '''creating the vehicle type df to convert the road type to id'''
    df_vehicle = create_table_road_vehicle_categories().data
    vehicle_dic = adp._create_dictionary_from_df(
        df_vehicle[['name', 'road_vehicle_category_id']]
        )
    df_convert['vehicle_type'] = adp._convert_id(
                                    df_convert['vehicle_type'], vehicle_dic)

    return(df_convert['vehicle_type'])


########################################################################
#                       table functions                                #
########################################################################


def create_table_road_area_categories():
    '''creating the road area table'''
    df = read_road_data()

    df_out = pd.DataFrame({
        'name': df['country'].unique()
    })

    df_out = _make_code(df_out)
    df_out = adp._create_id_col(df_out, 'road_area_category_id')

    table = adp.Table()
    table.data = df_out[['road_area_category_id', 'code', 'name']]
    table.name = 'road_area_categories'
    return(table)


def create_table_road_type_categories():
    '''creating the road type table'''
    df = read_road_data()

    df_out = pd.DataFrame({
        'name': df['road_type'].unique()
    })

    df_out = _make_code(df_out)
    df_out = adp._create_id_col(df_out, 'road_type_category_id')

    table = adp.Table()
    table.data = df_out[['road_type_category_id', 'code', 'name']]
    table.name = 'road_type_categories'
    return(table)


def create_table_road_vehicle_categories():
    '''creating the vehicle type table'''
    df = read_road_data()

    df_out = pd.DataFrame({
        'name': df['vehicle_type'].unique()
    })

    df_out = _make_code(df_out)
    df_out = adp._create_id_col(df_out, 'road_vehicle_category_id')

    table = adp.Table()
    table.data = df_out[['road_vehicle_category_id', 'code', 'name']]
    table.name = 'road_vehicle_categories'
    return(table)


def create_table_road_speed_profiles():
    '''creating the speed profile table. this consists of max speed,
    gradient and speed enforcement. speed enforcement is not currently
    used in uk aerius
    '''
    df = read_road_data()

    # a speed profile for UK aerius is the combo of max speed
    # and gradient
    df_out = df[['maximum_speed', 'gradient']]
    df_out.drop_duplicates(inplace=True)

    # this is not used in UK aerius
    df_out['speed_limit_enforcement'] = 'not_strict'
    df_out = adp._create_id_col(df_out, 'road_speed_profile_id')

    table = adp.Table()
    table.data = df_out[['road_speed_profile_id', 'speed_limit_enforcement',
                         'maximum_speed', 'gradient']]
    table.name = 'road_speed_profiles'
    return(table)


def create_table_road_areas_to_road_types():
    '''creates the combinations of road area to road type. ie london roads
    can only be found in the london area'''
    df = read_road_data()

    # we want onlt the unique combos of road_type and road_area
    df_out = df[['country', 'road_type']]
    df_out.drop_duplicates(inplace=True)

    # use the other create tables to match the id
    df_out['road_area_category_id'] = _convert_road_area(df_out)
    df_out['road_type_category_id'] = _convert_road_type(df_out)

    table = adp.Table()
    table.data = df_out[['road_area_category_id', 'road_type_category_id']]
    table.name = 'road_areas_to_road_types'
    return(table)


def create_table_road_types_to_speed_profiles():
    '''creates the combinations of road type with speed profile.
    we currently keep all combinations to aid with interpolation
    '''
    df = read_road_data()

    # we want only the unique combos of road_type and speed profile
    df_out = df[['maximum_speed', 'gradient', 'road_type']]
    df_out.drop_duplicates(inplace=True)

    # use the other create tables to match the id
    df_out['road_type_category_id'] = _convert_road_type(df_out)
    df_out['road_speed_profile_id'] = _convert_speed_profiles(df_out)

    table = adp.Table()
    table.data = df_out[['road_type_category_id', 'road_speed_profile_id']]
    table.name = 'road_types_to_speed_profiles'
    return(table)


def create_table_road_categories():
    '''creates the combos of all the above types to give a full
    road category
    '''
    df = read_road_data()

    # we want all the unique combos
    df_out = df[['maximum_speed', 'gradient', 'road_type',
                 'country', 'vehicle_type']]
    df_out.drop_duplicates(inplace=True)

    # use the other create tables to match the id
    df_out['road_speed_profile_id'] = _convert_speed_profiles(df_out)
    df_out['road_area_category_id'] = _convert_road_area(df_out)
    df_out['road_type_category_id'] = _convert_road_type(df_out)
    df_out['road_vehicle_category_id'] = _convert_vehicle_type(df_out)

    df_out = adp._create_id_col(df_out, 'road_category_id')

    table = adp.Table()
    table.data = df_out[[
        'road_category_id', 'road_area_category_id', 'road_type_category_id',
        'road_vehicle_category_id', 'road_speed_profile_id'
        ]]
    table.name = 'road_categories'
    return(table)


def create_table_road_category_emission_factors():
    '''gives the emissions factor for each year for each road category'''
    df = read_road_data()

    df_out = df[['maximum_speed', 'gradient', 'road_type', 'country',
                 'vehicle_type', 'emission_factor', 'year', 'substance_id']]
    # converts all the values to thier id by creating the appropriate
    # table from scratch and finding the id swap
    df_out['road_speed_profile_id'] = _convert_speed_profiles(df_out)
    df_out['road_area_category_id'] = _convert_road_area(df_out)
    df_out['road_type_category_id'] = _convert_road_type(df_out)
    df_out['road_vehicle_category_id'] = _convert_vehicle_type(df_out)

    # combine the ids with the ids in the categories table to get the
    # category id
    df_cat = create_table_road_categories().data
    df_final = pd.merge(df_out, df_cat, how='left')

    # stagnated emissions factor is not something used by UK aerius
    df_final['stagnated_emission_factor'] = df_final['emission_factor']
    df_final['substance_id'] = df_final['substance_id'].replace(
                                    'nox', adp.get_substance_id('nox'))
    df_final['substance_id'] = df_final['substance_id'].replace(
                                    'NH3', adp.get_substance_id('nh3'))

    table = adp.Table()
    table.data = df_final[['road_category_id', 'year', 'substance_id',
                           'emission_factor', 'stagnated_emission_factor']]
    table.name = 'road_category_emission_factors'
    return(table)

def create_table_standard_diurnal_variation_profiles(years):
    '''creates the diurnal road profiles table'''

    # convert years to str
    years = [ str(x) for x in years ]

    # create the entries for each year
    standard_diurnal_variation_profile_id=[]
    code=[]
    name=[]
    description=[]
    count=0
    for i in years:
        count +=1
        standard_diurnal_variation_profile_id.append(count)
        code.append('UK_ROAD_'+i)
        name.append('UK road '+i)
        description.append('Motor vehicle traffic distribution on all roads, Great Britain: '+i)

    # construct dataframe
    standard_diurnal_variation_profiles = pd.DataFrame(
        {'standard_diurnal_variation_profile_id': standard_diurnal_variation_profile_id,
         'code': code,
         'name': name,
         'description': description
         })

    # create table
    table = adp.Table()
    table.data = standard_diurnal_variation_profiles
    table.name = 'standard_diurnal_variation_profiles'
    return(table)

def create_table_standard_diurnal_variation_profiles_values(years):
    '''creates the diurnal road profiles values table'''
    # make all values ints
    years = [ int(x) for x in years]

    # read in the diurnal profiles csv
    diurnal_profiles = pd.read_excel(diurnal_path, sheet_name='TRA0307', skiprows=[0,1,2,3])

    # loop through columns to strip spaces from names
    for i in range(0,len(diurnal_profiles.columns)):
        name=diurnal_profiles.columns[i].strip()
        diurnal_profiles.rename(columns={ diurnal_profiles.columns[i]: name }, inplace = True)

    # subset only required years
    diurnal_profiles=diurnal_profiles[diurnal_profiles['Year'].isin(years)]

    # make empty dataframe to populate
    standard_diurnal_variation_profile_values =pd.DataFrame()

    # loop through each year
    count=0
    for i in years:
        count +=1
        #subste current year
        year_cut = diurnal_profiles[diurnal_profiles['Year']==i]
        # calculate relative values
        total=(year_cut[['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']].sum().sum())
        year_cut['relative_monday']=(year_cut['Monday']*168)/total
        year_cut['relative_tuesday']=(year_cut['Tuesday']*168)/total
        year_cut['relative_wednesday']=(year_cut['Wednesday']*168)/total
        year_cut['relative_thursday']=(year_cut['Thursday']*168)/total
        year_cut['relative_friday']=(year_cut['Friday']*168)/total
        year_cut['relative_saturday']=(year_cut['Saturday']*168)/total
        year_cut['relative_sunday']=(year_cut['Sunday']*168)/total
        # average weekdays
        year_cut['weekday_average']=(year_cut['relative_monday']+year_cut['relative_tuesday']+year_cut['relative_wednesday']+
                                     year_cut['relative_thursday']+year_cut['relative_friday'])/5

        # extract weekend
        year_cut_weekend=pd.DataFrame(year_cut['weekday_average'])
        year_cut_weekend.rename(columns={'weekday_average':'value'}, inplace=True)

        # extract saturdays
        year_cut_saturday=pd.DataFrame(year_cut['relative_saturday'])
        year_cut_saturday.rename(columns={'relative_saturday':'value'}, inplace=True)

        # extract sundays
        year_cut_sunday=pd.DataFrame(year_cut['relative_sunday'])
        year_cut_sunday.rename(columns={'relative_sunday':'value'}, inplace=True)

        #  group these
        year_cut_final=pd.concat([year_cut_weekend, year_cut_saturday], ignore_index=True)
        year_cut_final=pd.concat([year_cut_final, year_cut_sunday], ignore_index=True)

        # reset index
        year_cut_final.index = np.arange(1, len(year_cut_final) + 1)
        year_cut_final['standard_diurnal_variation_profile_id']=count
        year_cut_final['value_index']=year_cut_final.index

    # reorder columns
        year_cut_final=year_cut_final[['standard_diurnal_variation_profile_id','value_index','value']]

        # group outputs
        standard_diurnal_variation_profile_values=pd.concat([standard_diurnal_variation_profile_values, year_cut_final], ignore_index=True)

        # create table
    table = adp.Table()
    table.data = standard_diurnal_variation_profile_values
    table.name = 'standard_diurnal_variation_profile_values'
    return(table)


def create_table_sector_default_diurnal_variation_profiles():
    '''creates the default diurnal variation profile table'''

    # construct dataframe
    sector_default_diurnal_variation_profiles  = pd.DataFrame(
        {'sector_id': [3100],
         'code': ['UK_ROAD_2022']
         })

    # create table
    table = adp.Table()
    table.data = sector_default_diurnal_variation_profiles
    table.name = 'sector_default_diurnal_variation_profiles'
    return(table)


def create_table_sector_year_default_diurnal_variation_profiles(years):
    '''creates the default diurnal variation profile table'''

    # convert years to str
    years = [ str(x) for x in years ]

    # create the entries for each year
    sector_id=[]
    year=[]
    code=[]
    for i in years:
        sector_id.append(3100)
        year.append(i)
        code.append('UK_ROAD_'+i)

    # construct dataframe
    sector_year_default_diurnal_variation_profiles = pd.DataFrame(
        {'sector_id': sector_id,
         'year': year,
         'code': code
         })

    # create table
    table = adp.Table()
    table.data = sector_year_default_diurnal_variation_profiles
    table.name = 'sector_year_default_diurnal_variation_profiles'
    return(table)