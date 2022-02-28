import pandas as pd
import copy
import winsound

import lib_database_create as libdb


nox_path = './data/Road-emissions/full/nox.xlsx'
nh3_path = './data/Road-emissions/full/road_emissions_nh3.xlsx'


def read_road_data():

    df_nh3 = pd.read_excel(nh3_path)

    def expand_feature(df, name, expansion_list):

        var_dfs = []
        for var in expansion_list:
            df_var = copy.copy(df)
            df_var[name] = var
            var_dfs.append(df_var)

        return(pd.concat(var_dfs))

    df_nh3 = expand_feature(df_nh3, 'maximum_speed', [5, 16, 32, 48, 64, 70, 97, 113, 127, 140])
    df_nh3 = expand_feature(df_nh3, 'gradient', [0,1,2,3,4,5,6])

    df_nox = pd.read_excel(nox_path)
    # convert it to /s
    df_nox['emission_factor'] = df_nox['emission_factor'] / 86400
    df = pd.concat([df_nh3, df_nox])

    #TODO improve this so no ifs. replace 100 with col name + add all cols together
    def get_vehicle_type(df):
        if df['Car'] == 100.0:
            return('Car')
        if df['Taxi (black cab)'] == 100.0:
            return('Taxi (black cab)')
        if df['LGV'] == 100.0:
            return('LGV')
        if df['HGV'] == 100.0:
            return('HGV')
        if df['Bus and Coach'] == 100.0:
            return('Bus and Coach')
        if df['Motorcycle'] == 100.0:
            return('Motorcycle')

    df['vehicle_type'] = df.apply(lambda df: get_vehicle_type(df), axis=1)
    df['maximum_speed'] = df['maximum_speed'].astype(int).astype(str)
    df['year'] = df['year'].astype(int).astype(str)

    return(df)

def _make_code(df_code):

    def _code_start(row):
        return(row['name'][0:3])

    df_code['code'] = df_code.apply(lambda df_code: _code_start(df_code), axis=1)
    g = df_code.groupby('code')
    df_code['code'] += g.cumcount().add(1).astype(str).mask(g['code'].transform('count')==1,'')
    return(df_code)


def _convert_speed_profiles(df_convert):

    # need to have a single column to convert to ID
    df_convert['speed_profile'] = df_convert['maximum_speed'].astype(str) + '-' + df_convert['gradient'].astype(str)
    # Changing the countries to the ids created in the speed profiles table
    df_speed = create_table_road_speed_profiles()
    df_speed['speed_profile'] = df_speed['maximum_speed'].astype(str) + '-' + df_speed['gradient'].astype(str)
    speed_dic = libdb._create_dictionary_from_df(df_speed[['speed_profile', 'road_speed_profile_id']])
    df_convert['road_speed_profile_id'] = libdb._convert_id(df_convert['speed_profile'], speed_dic)
    return(df_convert['road_speed_profile_id'])

def _convert_road_type(df_convert):

    # Changing the road_types to the ids created in the road type table
    df_types = create_table_road_type_categories()
    type_dic = libdb._create_dictionary_from_df(df_types[['name', 'road_type_category_id']])
    df_convert['road_type'] = libdb._convert_id(df_convert['road_type'], type_dic)
    return(df_convert['road_type'])

def _convert_road_area(df_convert):
    # Changing the countries to the ids created in the road
    # area table
    df_areas = create_table_road_area_categories()
    areas_dic = libdb._create_dictionary_from_df(df_areas[['name', 'road_area_category_id']])
    df_convert['country'] = libdb._convert_id(df_convert['country'], areas_dic)
    return(df_convert['country'])

def _convert_vehicle_type(df_convert):
    # Changing the countries to the ids created in the road
    # area table
    df_vehicle = create_table_road_vehicle_categories()
    vehicle_dic = libdb._create_dictionary_from_df(df_vehicle[['name', 'road_vehicle_category_id']])
    df_convert['vehicle_type'] = libdb._convert_id(df_convert['vehicle_type'], vehicle_dic)
    return(df_convert['vehicle_type'])


########################################################################
###                     table functions                              ###
########################################################################


def create_table_road_area_categories():

    df = read_road_data()

    df_out = pd.DataFrame({
        'name': df['country'].unique()
    })
    
    df_out = _make_code(df_out)
    df_out = libdb._create_id_col(df_out, 'road_area_category_id')

    return(df_out[['road_area_category_id', 'name', 'code']])


def create_table_road_type_categories():

    df = read_road_data()

    df_out = pd.DataFrame({
        'name': df['road_type'].unique()
    })
    
    df_out = _make_code(df_out)
    df_out = libdb._create_id_col(df_out, 'road_type_category_id')

    return(df_out[['road_type_category_id', 'name', 'code']])

def create_table_road_vehicle_categories():

    df = read_road_data()

    df_out = pd.DataFrame({
        'name': df['vehicle_type'].unique()
    })
    
    df_out = _make_code(df_out)
    df_out = libdb._create_id_col(df_out, 'road_vehicle_category_id')

    return(df_out[['road_vehicle_category_id', 'name', 'code']])


def create_table_road_speed_profiles():

    df = read_road_data()

    df_out = df[['maximum_speed', 'gradient']]
    df_out.drop_duplicates(inplace=True)

    df_out['speed_limit_enforcement'] = 'irrelevant'
    df_out = libdb._create_id_col(df_out, 'road_speed_profile_id')

    return(df_out[['road_speed_profile_id', 'speed_limit_enforcement', 'maximum_speed', 'gradient']])


def create_table_road_areas_to_road_types():

    df = read_road_data()
    # we want onlt the unique combos of road_type and road_area
    df_out = df[['country','road_type']]
    df_out.drop_duplicates(inplace=True)

    df_out['road_area_category_id'] = _convert_road_area(df_out)
    df_out['road_type_category_id'] = _convert_road_type(df_out)

    return(df_out[['road_area_category_id', 'road_type_category_id']])

def create_table_road_types_to_speed_profiles():

    df = read_road_data()
    # we want onlt the unique combos of road_type and speed profile
    df_out = df[['maximum_speed','gradient', 'road_type']]
    df_out.drop_duplicates(inplace=True)

    df_out['road_type_category_id'] = _convert_road_type(df_out)
    df_out['road_speed_profile_id'] = _convert_speed_profiles(df_out)

    return(df_out[['road_type_category_id', 'road_speed_profile_id']])

def create_table_road_categories():

    df = read_road_data()
    # we want all the unique combos
    df_out = df[['maximum_speed','gradient', 'road_type', 'country', 'vehicle_type']]
    df_out.drop_duplicates(inplace=True)

    df_out['road_speed_profile_id'] = _convert_speed_profiles(df_out)
    df_out['road_area_category_id'] = _convert_road_area(df_out)
    df_out['road_type_category_id'] = _convert_road_type(df_out)
    df_out['road_vehicle_category_id'] = _convert_vehicle_type(df_out)
    
    df_out = libdb._create_id_col(df_out, 'road_category_id')

    return(df_out[['road_category_id', 'road_area_category_id', 'road_type_category_id', 'road_vehicle_category_id', 'road_speed_profile_id']])


def create_table_road_category_emission_factors():

    df = read_road_data()
    
    df_out = df[['maximum_speed','gradient', 'road_type', 'country', 'vehicle_type', 'emission_factor', 'year', 'substance_id']]
    df_out['road_speed_profile_id'] = _convert_speed_profiles(df_out)
    df_out['road_area_category_id'] = _convert_road_area(df_out)
    df_out['road_type_category_id'] = _convert_road_type(df_out)
    df_out['road_vehicle_category_id'] = _convert_vehicle_type(df_out)

    df_cat = create_table_road_categories()
    print(df_cat.columns)
    print(df_out.columns)
    df_final = pd.merge(df_out, df_cat, how='left')

    df_final['stagnated_emission_factor'] = df_final['emission_factor']
    df_final['substance_id'] = df_final['substance_id'].replace('nox', libdb.get_substance_id('nox'))
    df_final['substance_id'] = df_final['substance_id'].replace('NH3', libdb.get_substance_id('NHx'))

    return(df_final[['road_category_id', 'year', 'substance_id', 'emission_factor', 'stagnated_emission_factor']])


########################################################################
###                     the old tables                               ###
########################################################################


# def prep_table_road_categories():

#     df = read_road_data()

#     df_cat = df[['country', 'vehicle_type']]
#     df_cat.drop_duplicates(inplace=True)

#     df_cat['gcn_sector_id'] = 1
#     df_cat['name'] = df_cat['vehicle_type'] + ' (' + df_cat['country'] + ')'
#     df_cat['description'] = df_cat['vehicle_type'] + '(' + df_cat['country'] + ')'

#     df_cat = libdb._create_id_col(df_cat, 'road_category_id')

#     return(df_cat[['road_category_id', 'gcn_sector_id', 'country', 'vehicle_type', 'name', 'description']])
# def create_table_road_speed_profiles():

#     df = read_road_data()

#     df_road_speed = df[['road_type', 'maximum_speed']]
#     df_road_speed.drop_duplicates(inplace=True)

#     df_road_speed['speed_limit_enforcement'] = 'irrelevant'
#     df_road_speed['name'] = df_road_speed['road_type'] + ' - ' + df_road_speed['maximum_speed']

#     df_road_speed = libdb._create_id_col(df_road_speed, 'road_speed_profile_id')

#     return(df_road_speed[['road_speed_profile_id', 'road_type', 'speed_limit_enforcement', 'maximum_speed', 'name']])

# def create_table_road_categories():

#     df = prep_table_road_categories()
#     # the prep table can't be road type as there is another road type
#     return(df.rename(columns={'country':'road_type'}))

# def create_table_road_category_emission_factors():

#     df = read_road_data()
#     df_speed = create_table_road_speed_profiles()
#     df_cat = prep_table_road_categories()

#     df = pd.merge(df, df_speed[['road_speed_profile_id', 'road_type', 'maximum_speed']], how='left')
#     df = pd.merge(df, df_cat[['road_category_id', 'country', 'vehicle_type']], how='left')

#     # Convert from kg to g
#     df['emission_factor'] = df['emission_factor'] * 1000

#     df['stagnated_emission_factor'] = df['emission_factor']
#     df['substance_id'] = df['substance_id'].replace('NOx', libdb.get_substance_id('nox'))
#     df['substance_id'] = df['substance_id'].replace('NH3', libdb.get_substance_id('NHx'))

#     return(df[['road_category_id', 'road_speed_profile_id', 'year', 'substance_id', 'emission_factor', 'stagnated_emission_factor']])


########################################################################

if __name__ == "__main__":

    output_c = './output/aerius_data_22-02-23v2/'

    # df_area = create_table_road_area_categories()
    # df_area.to_csv(output_c+'road_area_categories.txt', index=False, sep='\t')
    # df_types = create_table_road_type_categories()
    # df_types.to_csv(output_c+'road_type_categories.txt', index=False, sep='\t')
    # df_vehicle = create_table_road_vehicle_categories()
    # df_vehicle.to_csv(output_c+'road_vehicle_categories.txt', index=False, sep='\t')
    # df_speed = create_table_road_speed_profiles()
    # df_speed.to_csv(output_c+'road_speed_profiles.txt', index=False, sep='\t')

    df_area_to_type = create_table_road_areas_to_road_types()
    df_area_to_type.to_csv(output_c+'road_areas_to_road_types.txt', index=False, sep='\t')
    # df_type_to_speed = create_table_road_types_to_speed_profiles()
    # df_type_to_speed.to_csv(output_c+'road_types_to_speed_profiles.txt', index=False, sep='\t')
    # df_cat = create_table_road_categories()
    # df_cat.to_csv(output_c+'road_categories.txt', index=False, sep='\t')

    # df_emission = create_table_road_category_emission_factors()
    # df_emission.to_csv(output_c+'road_category_emission_factors.txt', index=False, sep='\t')

    #winsound.Beep(2500, 1000)
