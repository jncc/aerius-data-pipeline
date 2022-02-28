from operator import index
import unicodedata
import copy
from os import sep # for finding the name of the function to pass as errors
import pyodbc
import re

import shapely
# for going through the sites and lpas one by one
from shapely.strtree import STRtree
from pathlib import Path
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon

import numpy as np
import pandas as pd
import geopandas as gpd

import lib_database_create as libdb

########################################################################
# global variables
########################################################################

data = "./data/linkages_table/"
critical_levels = "APIS-interest-feature-critical-load-linkages_simplBB.xlsx"
levels_tab = "LINKAGES-FEATURE to CLOADS"

links = 'feature_contry_v2.csv'

lpa_shape = './data/Local_Planning_Authorities/Local_Planning_Authorities_(April_2019)_UK_BUC.shp'


sens_cols = ['INTERESTCODE', 'INTERESTNAME', 'INTERESTLAYNAME', 'INTERESTTYPECODE', 
        'INTERESTTYPE', 'HABITATCODE', 'BROADHABITAT', 'NVC_CODE',
        'EUNISCODE', 'NCLCODE', 'NCLCLASS', 'ACCODE', 'ACIDITYCLASS']

code_cols = ['INTERESTLAYNAME', 'INTERESTTYPECODE', 'HABITATCODE', 'NVC_CODE',
        'EUNISCODE', 'NCLCODE', 'ACCODE']

substance_rename = {
    'NDEP_LEVEL': libdb.get_substance_id('ndep'),
    'SPECIESSENSITIVITYN': libdb.get_substance_id('ndep'),
    'SPECIESSENSITIVEN': libdb.get_substance_id('ndep'),
    'ACCODE': libdb.get_substance_id('adep'),
    'SPECIESSENSITIVITYA': libdb.get_substance_id('adep'),
    'SPECIESSENSITIVEA': libdb.get_substance_id('adep'),
    'NH3_CLEVEL': libdb.get_substance_id('nh3'),
    'JUSTIF_SPECIES_SENSITIVE_NH3': libdb.get_substance_id('nh3'),
    'SPECIES_SENSITIVE_NH3': libdb.get_substance_id('nh3'),
    'NOX_CLEVEL': libdb.get_substance_id('nox'), 
    'JUSTIF_SPECIES_SENSITIVE_NOX': libdb.get_substance_id('nox'),
    'SPECIES_SENSITIVE_NOX': libdb.get_substance_id('nox'),
    'SO2_CLEVEL': libdb.get_substance_id('so2'),
    'JUSTIF_SPECIES_SENSITIVE_SO2': libdb.get_substance_id('so2'),
    'SPECIES_SENSITIVE_SO2': libdb.get_substance_id('so2')
}

########################################################################
# Setup functions
########################################################################

def import_feat_sens(data_dir=data, file_name=critical_levels, tab_name=levels_tab):
    '''boilerplate for extracting a useful dataframe from the linkages
    excel file
    '''

    df = pd.read_excel(data_dir+file_name, tab_name)
    #sorting by feture to create a consistent id set
    df = df.sort_values('INTERESTCODE')
    # There are duplicate rows in the linkages table
    df = df.drop_duplicates()
    # converting the string columns to string
    all_cols = list(df.columns.values)
    df[all_cols] = df[all_cols].astype(str)

    return(df)


def clean_feat(df):

    def _norm_code_col(val):
        return(unicodedata.normalize("NFKD", val))

    # getting rid of the \x0 characters
    df = df.applymap(_norm_code_col)
    # getting rid of trailing and leading spaces
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    df = df.replace("’","", regex=True)

    return(df)


def make_feat_codes(df):

    
    df["EUNISCODE"].replace({"nan": 'EU000', 'D2 f': 'D2', 'information to be added': 'EU000',
                            "Broad-leaved, mixed and yew woodland (Scrub - Pteridium aquilinum-Rubus fruticosus underscrub)": 'EU000',
                            "Broad-leaved, mixed and yew woodland (Scrub - Rubus fruticosus-Holcus lanatus underscrub)": 'EU000'}, inplace=True)


    df["NVC_CODE"].replace({"nan": 'NVC000'}, inplace=True)
    df["NVC_CODE_original"] = df["NVC_CODE"]

    nvc_types = df['NVC_CODE'].unique().tolist()
    nvc_types.remove('NVC000')
    nvc_to = [f'NVC{i}' for i in range(1, len(nvc_types))]
    nvc_rename = dict(zip(nvc_types, nvc_to))

    df["NVC_CODE"].replace(nvc_rename, inplace=True)

    

    df['name'] = df['INTERESTLAYNAME'] + ' - ' + df['INTERESTTYPE']
    df['name'] = df['name'].str.replace(' - Habitat','')
    g = df.groupby('name')
    df['name'] += g.cumcount().add(1).astype(str).radd(' ').mask(g['name'].transform('count')==1,'')

    df = df.drop_duplicates()

    df['description'] = df['HABITATCODE'] + ':' + df['NVC_CODE'] + ':' + df['EUNISCODE'] + ':' + df['NCLCODE'] + ':' + df['ACCODE']
    df = libdb._create_id_col(df, 'habitat_type_id')

    

    return(df)

def _unstack_sens_data(df_sens_ready, level, sensitivity):
    '''input the dataframe, and two lists. one of the levels columns,
    the second of the sensitivity columns. make sure that the id column
    is at the beginning of each list. outputs a dataframe.
    '''

    id_string = level[0]

    df_an_load = df_sens_ready[level]
    df_an_load.set_index(id_string, inplace=True)
    df_an_load = df_an_load.T.unstack().reset_index().rename(columns={'level_1':'substance_id', 0: 'critical_level'})
    #df_an_load = _convert_substance_id(df_an_load)
    df_an_load['substance_id'] = libdb._convert_id(df_an_load['substance_id'], substance_rename)

    df_an_sens = df_sens_ready[sensitivity]
    df_an_sens.set_index(id_string, inplace=True)
    df_an_sens = df_an_sens.T.unstack().reset_index().rename(columns={'level_1':'substance_id', 0: 'sensitivity'})
    #df_an_sens = _convert_substance_id(df_an_sens)
    df_an_sens['substance_id'] = libdb._convert_id(df_an_sens['substance_id'], substance_rename)

    df_an = pd.merge(df_an_load, df_an_sens, on=[id_string, 'substance_id'], how='outer')

    return(df_an)

def import_sites_from_shape():

    df_sac = gpd.read_file('data\Sites\ SAC_BNG.shp')
    df_sac['design_status_description'] = 'SAC'
    #FIXME keeping the double country info rather than dropping it
    df_sac.drop_duplicates('SITECODE', inplace=True)
    df_spa = gpd.read_file('data\Sites\ SPA_BNG.shp')
    df_spa['design_status_description'] = 'SPA'
    df_spa.drop_duplicates('SITECODE', inplace=True)
    df_sssi = gpd.read_file('data\Sites\ SSSI_BNG.shp')
    df_sssi['design_status_description'] = 'SSSI'
    df_sssi.drop_duplicates('SITECODE', inplace=True)

    # Converting polygons to multipolygons using shapely
    # needed to fit into the db
    df_all = gpd.GeoDataFrame(pd.concat([df_sac, df_spa, df_sssi], ignore_index=True))
    df_all = libdb._create_id_col(df_all, 'assessment_area_id')

    df_all["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon \
        else feature for feature in df_all["geometry"]]
 
    return(df_all)

def get_lpa_geoms(df_sites, lpa_path=lpa_shape):

    # if the table is read from txt rather than file
    # def make_shapely(df):
    #     return(shapely.wkt.loads(df['geometry']))
    # the text geoms will need to be converted to shapely geoms
    #df_sites['geom_shape'] = df_sites.apply(lambda df: make_shapely(df), axis=1)

    df_lpa = gpd.read_file(lpa_path)
    lpa_tree = STRtree(df_lpa['geometry'].tolist())

    def find_lpas(df_site_r):

        # The version where the geomshape is made from text
        #site_geom = df_site_r['geom_shape']
        site_geom = df_site_r['geometry']
        find_geoms = [o for o in lpa_tree.query(site_geom) if o.intersects(site_geom)]

        site_lpa_list = []
        for geo in find_geoms:
            site_lpa_list.append(df_lpa.loc[df_lpa.index[df_lpa['geometry'] == geo].values[0], 'lpa19nm'])

        # some of the sites are offshore and don't have an LPA so they will error below
        if not site_lpa_list:
            return('marine')

        #TODO This is only the first in the list as the table only accepts a single id
        return(site_lpa_list[0])

    df_sites['authority_id'] = df_sites.apply(lambda df: find_lpas(df), axis=1)
    # We don't want marine sites in aerius
    df_sites = df_sites[df_sites['authority_id'] != 'marine']

    authority_dict = libdb._create_dictionary_from_df(create_table_authorities(lpa_shape)[['name', 'authority_id']])
    df_sites['authority_id'] = libdb._convert_id(df_sites['authority_id'], authority_dict)

    return(df_sites)

def import_links(data_dir=data, links_file=links):

    df = pd.read_csv(data_dir+links_file)
    df = df[df['COUNTRY'].notna()]

    return(df)

def setup_natura_tables():

    df_site = import_sites_from_shape()
    df_site = get_lpa_geoms(df_site)

    df_site = df_site.rename(columns={'SITECODE': 'code', 'SITENAME': 'name'})
    
    df_site['assessment_area_id'] = df_site['assessment_area_id'].astype(int)
    df_site['natura2000_area_id'] = df_site['assessment_area_id']

    return(df_site)

########################################################################
# Create table functions
########################################################################

def create_table_habitat_type_critical_levels():

    df = import_feat_sens()
    df = clean_feat(df)
    df = make_feat_codes(df)

    df['NDEP_LEVEL'] = df['CLMIN_NUTN'] + ' to ' + df['CLMAX_NUTN']

    # unstack data turns the columns into a single column and the values 
    # into the next column
    df_an = _unstack_sens_data(
        df, 
        ['habitat_type_id', 'NDEP_LEVEL', 'ACCODE'], 
        ['habitat_type_id', 'SPECIESSENSITIVEN', 'SPECIESSENSITIVEA']
        )
    df_an['result_type'] = 'deposition'

    df_nns = _unstack_sens_data(
        df, 
        ['habitat_type_id', 'NH3_CLEVEL', 'NOX_CLEVEL', 'SO2_CLEVEL'], 
        ['habitat_type_id', 'SPECIES_SENSITIVE_NH3', 'SPECIES_SENSITIVE_NOX', 'SPECIES_SENSITIVE_SO2']
        )
    df_nns['result_type'] = 'concentration'

    hab_sensitivity = df_an.append(df_nns).sort_values(['habitat_type_id', 'substance_id'])
    hab_sensitivity.reset_index(drop=True, inplace=True)

    # THIS IS THE CORRECT CODE!!!!!!!
    # just the data doesn't have the info yet for habitats
    # hab_sensitivity['sensitivity'] = hab_sensitivity['sensitivity'].replace({
    #     'Site specific': 't',
    #     'Yes': 't',
    #     'No': 'f'
    # })

    #FIXME temporary fix until the data it right
    hab_sensitivity.loc[hab_sensitivity['critical_level'] != 'nan', 'sensitivity'] = 't'
    hab_sensitivity.loc[hab_sensitivity['critical_level'].isin(['nan', 'nan to nan']), 'sensitivity'] = 'f'

    # gets rid of the acid rows for now
    hab_sensitivity = hab_sensitivity[hab_sensitivity['substance_id'] != libdb.get_substance_id('adep')]
    
    # getting the first value from the range of loads/levels
    hab_sensitivity['critical_range'] = hab_sensitivity['critical_level']
    hab_sensitivity['critical_level'] = hab_sensitivity['critical_level'].str.split(' to ').str[0]
    hab_sensitivity['critical_level'] = hab_sensitivity['critical_level'].str.split(' or ').str[0]

    hab_sensitivity['sensitive'] = hab_sensitivity['sensitivity']
    hab_sensitivity['habitat_type_id'] = hab_sensitivity['habitat_type_id']

    hab_sensitivity = hab_sensitivity.loc[~hab_sensitivity['substance_id'].isin([1000, '1000'])]

    return(hab_sensitivity[['habitat_type_id', 'substance_id', 'result_type', 'critical_level', 'sensitive']])

def create_table_authorities(path):

    df_lpa = gpd.read_file(path)

    df_lpa['country_id'] = [x[0] for x in df_lpa['lpa19cd']]

    # getting the country from the code and converting to id
    countries = libdb.create_table_countries()
    country_dict = libdb._create_dictionary_from_df(countries[['code', 'country_id']])
    df_lpa['country_id'] = libdb._convert_id(df_lpa['country_id'], country_dict)

    df_lpa = df_lpa.rename(columns={'objectid':'authority_id', 
                                    'lpa19cd': 'code',
                                    'lpa19nm':'name'})

    df_lpa['type'] = 'unknown'

    return(df_lpa[['authority_id', 'country_id', 'code', 'name', 'type']])

def create_table_habitat_areas():

    df_l = import_links()

    df_f = import_feat_sens()
    df_f = clean_feat(df_f)
    df_f = make_feat_codes(df_f)

    df_s = import_sites_from_shape()
    df_s = get_lpa_geoms(df_s)

    df_full = pd.merge(df_l, df_f[['INTERESTCODE', 'INTERESTTYPECODE', 'habitat_type_id']], how='outer', on=['INTERESTCODE', 'INTERESTTYPECODE'])
    df_full = df_full[df_full['habitat_type_id'].notna()]
    df_full = pd.merge(df_full, df_s[['SITECODE', 'assessment_area_id', 'geometry']], how='inner')

    df_full = libdb._create_id_col(df_full, 'habitat_area_id')
    df_full['coverage'] = 1

    return(df_full[['assessment_area_id', 'habitat_area_id', 'habitat_type_id', 'coverage', 'geometry']])

def create_table_natura2000_areas():

    df = setup_natura_tables()

    df['type'] = 'natura2000_area'
    #df['type'] = df['design_status_description']
    return(df[['assessment_area_id', 'type', 'name', 'code', 'authority_id', 'geometry', 'natura2000_area_id']])

def create_table_natura2000_directive_areas():

    df = setup_natura_tables()

    df['type'] = 'natura2000_directive_area'
    #df['type'] = df['design_status_description']

    def assign_bird_dir(df):
        if df['design_status_description'] == 'SPA':
            return('t')
        return('f')
    df['bird_directive'] = df.apply(lambda df: assign_bird_dir(df), axis=1)

    def assign_hab_dir(df):
        if df['design_status_description'] == 'SAC':
            return('t')
        return('f')
    df['habitat_directive'] = df.apply(lambda df: assign_hab_dir(df), axis=1)

    #df['bird_directive'] = 'f'
    #df['habitat_directive'] = 'f'

    df['assessment_area_id'] = df['assessment_area_id'] + 1000000
    df['natura2000_directive_area_id'] = df['assessment_area_id']
    return(df[['assessment_area_id', 'type', 'name', 'code', 'authority_id', 
                'geometry', 'natura2000_directive_area_id', 'natura2000_area_id', 
                'bird_directive', 'habitat_directive', 'design_status_description']])

def create_table_habitat_types():

    df_feat = import_feat_sens()
    df_feat = clean_feat(df_feat)
    df_feat = make_feat_codes(df_feat)
    return(df_feat[['habitat_type_id', 'name', 'description']])

########################################################################
# main script
########################################################################

if __name__ == "__main__":

    output = './output/aerius_data_22-02-28/'

    df_feat = create_table_habitat_types()
    df_feat.to_csv(output+'habitat_types.txt', sep='\t', index=False)

    df_sens = create_table_habitat_type_critical_levels()
    df_sens.to_csv(output+'habitat_type_critical_levels.txt', sep='\t', index=False)

    df_areas = create_table_habitat_areas()
    df_areas.to_csv(output+'habitat_areas.txt', sep='\t', index=False)

    df_nat = create_table_natura2000_areas()
    df_nat.to_csv(output+'natura2000_areas.txt', sep='\t', index=False)

    df_nat_dir = create_table_natura2000_directive_areas()
    df_nat_dir.to_csv(output+'natura2000_directive_areas.txt', sep='\t', index=False)


    # Checking for the feats that are not in the sens table
    #pd.DataFrame({'missing_feats': df_link[df_link['habitat_type_id'].isna()]['INTERESTCODE'].unique()}).to_csv('missing_feats.csv', index=False)
    
    # The sites that are not found in the shapefile
    # missing_sites = list(set(df_link['SITECODE'].unique()) - set(df_site['SITECODE'].unique()))
    # print(missing_sites)
    # print(len(missing_sites))
    # df_all_sites = pd.read_csv('./data/linkages_table/sitename_code.csv', encoding = "ISO-8859-1")
    # df_all_sites = df_all_sites[df_all_sites['SITECODE'].isin(missing_sites)]
    # df_all_sites.to_csv('missing_all_sites.csv', index=False)
