
def _create_dictionary_from_df(df):
    '''creates a dictionary for name swaps beteen the code and the name.
    Takes a dictionary with only two columns as argument. The first col
    will be the keys, the second will be the values
    '''
    print('lib_database_create_script+_create_dictionary_from_df')
    df = df.drop_duplicates()
    # creating the dictionary from the two columns of the dataframe
    # to use to_dict, the index should be the keys
    feat_name_dict = pd.Series(df[df.columns[1]].values,
                               index=df[df.columns[0]]).to_dict()

    return(feat_name_dict)


def _create_id_col(df, id_colname):
    '''creates a column for the db tables with a unique ID'''
    print('lib_database_create_script+_create_id_col')
    # _check_unique_rows(df)

    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df[id_colname] = df.index
    df[id_colname] = df[id_colname] + 1
    df[id_colname] = df[id_colname].astype(str)
    return(df)


def _convert_id(df_col, rename):
    '''converts colnames into substanc_id with dictionary'''
    print('lib_database_create_script+_convert_id')
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
    print('lib_database_create_script+_check_unique_rows')
    cols = list(df.columns)
    # if these are the same then all rows are unique IDs
    if df.shape[0] == df.groupby(cols).ngroups:
        return()

    # inspect.stack()[1][3] gives the name of the function that called
    print('\n!!!!!!!Warning!!!!!!!!!!!!\n'
          + str(df.shape[0] - df.groupby(cols).ngroups)
          + ' rows in '+inspect.stack()[2][3]+' are duplicated.\n')
    duplicate = df[df.duplicated(cols)]
    print(duplicate)

########################################################################
# Useful functions
########################################################################


def get_id(df, col_ref, col_id, name):
    print('lib_database_create_script+get_id')
    return(df.loc[df[col_ref] == name, col_id].values[0])


def get_substance_id(name):
    print('lib_database_create_script+get_substance_id')
    '''a way to consistently get the right substance id'''
    df = create_table_substances().data
    return(df.loc[df['name'] == name, 'substance_id'].values[0])

########################################################################


def create_table_substances():
    '''Creates the substance table. Takes no argument and returns a df'''
    print('lib_database_create_script+create_table_substances')
    # Data frame created like this to link the id with name
    # we dont want them getting mixed as more are added or taken away
    all_sub = np.array([
        [1, 'so2', 'sulphur dioxide'],
        [2,	'co2',	'carbon dioxide'],
        [4,	'co',	'carbon monoxide'],
        [7,	'no2',	'nitrogen dioxide'],
        [8,	'bap',	'benzo(a)pyrene'],
        [9,	'c6h6',	'benzene'],
        [10, 'pm10',	'particulate matter 10'],
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
        [1000, 'adep', 'acid deposition']
        ])

    table = Table()
    table.data = pd.DataFrame(
        all_sub, columns=['substance_id', 'name', 'description']
        )
    table.name = 'substances'
    return(table)


def create_table_example_authorities():
    '''Creates the substance table. Takes no argument and returns a df'''
    print('lib_database_create_script+create_table_example_authorities')
    # Data frame created like this to link the id with name
    # we dont want them getting mixed as more are added or taken away
    all_sub = np.array([
        [1003, 4, 'City of Belfast',	'City of Belfast', 'unknown'],
        [1015, 1, 'Bedford (B)',	'Bedford (B)', 'unknown'],
        [1108, 3, 'Gwynedd - Gwynedd', 'Gwynedd - Gwynedd', 'unknown'],
        [1165, 2, 'Shetland Islands',	'Shetland Islands',	'unknown']
        ])

    return(pd.DataFrame(
        all_sub,
        columns=['authority_id', 'country_id', 'code', 'name', 'type']
        ))


def create_table_countries():
    '''Creates the table for countries, all manually put entered'''
    print('lib_database_create_script+create_table_countries')
    all_sub = np.array([[1, 'E', 'England'],
                        [2, 'S', 'Scotland'],
                        [3, 'W', 'Wales'],
                        [4, 'N', 'Northern Ireland'],
                        [5, 'SE', 'Scotland/England'],
                        [6, 'WE', 'Wales/England']])

    table = Table()
    table.data = pd.DataFrame(all_sub,
                              columns=['country_id', 'code', 'name'])
    table.name = 'countries'
    return(table)

def _create_dictionary_from_df(df):
    '''creates a dictionary for name swaps beteen the code and the name.
    Takes a dictionary with only two columns as argument. The first col
    will be the keys, the second will be the values
    '''
    print('lib_database_create_script+_create_dictionary_from_df')
    df = df.drop_duplicates()
    # creating the dictionary from the two columns of the dataframe
    # to use to_dict, the index should be the keys
    feat_name_dict = pd.Series(df[df.columns[1]].values,
                               index=df[df.columns[0]]).to_dict()

    return(feat_name_dict)


def _create_id_col(df, id_colname):
    '''creates a column for the db tables with a unique ID'''
    print('lib_database_create_script+_create_id_col')
    # _check_unique_rows(df)

    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df[id_colname] = df.index
    df[id_colname] = df[id_colname] + 1
    df[id_colname] = df[id_colname].astype(str)
    return(df)


def _convert_id(df_col, rename):
    '''converts colnames into substanc_id with dictionary'''
    print('lib_database_create_script+_convert_id')
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
    print('lib_database_create_script+_check_unique_rows')
    cols = list(df.columns)
    # if these are the same then all rows are unique IDs
    if df.shape[0] == df.groupby(cols).ngroups:
        return()

    # inspect.stack()[1][3] gives the name of the function that called
    print('\n!!!!!!!Warning!!!!!!!!!!!!\n'
          + str(df.shape[0] - df.groupby(cols).ngroups)
          + ' rows in '+inspect.stack()[2][3]+' are duplicated.\n')
    duplicate = df[df.duplicated(cols)]
    print(duplicate)

########################################################################
# Useful functions
########################################################################


def get_id(df, col_ref, col_id, name):
    print('lib_database_create_script+get_id')
    return(df.loc[df[col_ref] == name, col_id].values[0])


def get_substance_id(name):
    print('lib_database_create_script+get_substance_id')
    '''a way to consistently get the right substance id'''
    df = create_table_substances().data
    return(df.loc[df['name'] == name, 'substance_id'].values[0])

########################################################################


def create_table_substances():
    '''Creates the substance table. Takes no argument and returns a df'''
    print('lib_database_create_script+create_table_substances')
    # Data frame created like this to link the id with name
    # we dont want them getting mixed as more are added or taken away
    all_sub = np.array([
        [1, 'so2', 'sulphur dioxide'],
        [2,	'co2',	'carbon dioxide'],
        [4,	'co',	'carbon monoxide'],
        [7,	'no2',	'nitrogen dioxide'],
        [8,	'bap',	'benzo(a)pyrene'],
        [9,	'c6h6',	'benzene'],
        [10, 'pm10',	'particulate matter 10'],
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
        [1000, 'adep', 'acid deposition']
        ])

    table = Table()
    table.data = pd.DataFrame(
        all_sub, columns=['substance_id', 'name', 'description']
        )
    table.name = 'substances'
    return(table)


def create_table_example_authorities():
    '''Creates the substance table. Takes no argument and returns a df'''
    print('lib_database_create_script+create_table_example_authorities')
    # Data frame created like this to link the id with name
    # we dont want them getting mixed as more are added or taken away
    all_sub = np.array([
        [1003, 4, 'City of Belfast',	'City of Belfast', 'unknown'],
        [1015, 1, 'Bedford (B)',	'Bedford (B)', 'unknown'],
        [1108, 3, 'Gwynedd - Gwynedd', 'Gwynedd - Gwynedd', 'unknown'],
        [1165, 2, 'Shetland Islands',	'Shetland Islands',	'unknown']
        ])

    return(pd.DataFrame(
        all_sub,
        columns=['authority_id', 'country_id', 'code', 'name', 'type']
        ))


def create_table_countries():
    '''Creates the table for countries, all manually put entered'''
    print('lib_database_create_script+create_table_countries')
    all_sub = np.array([[1, 'E', 'England'],
                        [2, 'S', 'Scotland'],
                        [3, 'W', 'Wales'],
                        [4, 'N', 'Northern Ireland'],
                        [5, 'SE', 'Scotland/England'],
                        [6, 'WE', 'Wales/England']])

    table = Table()
    table.data = pd.DataFrame(all_sub,
                              columns=['country_id', 'code', 'name'])
    table.name = 'countries'
    return(table)


from pathlib import Path


class Table():
    """Exporter for data structures into txt files for db.
    Input the overall output path
    """

    def __init__(self):
        print('export_script+__init__')
        self.name = None
        self.data = None

    def export_data(self, filepath):
        print('export_script+export_data')
        # errors for missing name and data

        root = f'./output/{filepath}/'
        print(f'Exporting file {self.name} to {root}\n')
        Path(root).mkdir(parents=True, exist_ok=True)
        self.data.to_csv(f'{root}/{self.name}.txt', sep='\t', index=False)



import unicodedata
# from os import sep  # for finding the name of the function to pass as errors

# for going through the sites and lpas one by one
from shapely.strtree import STRtree
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon

import pandas as pd
import numpy as np
import geopandas as gpd
# used for checking purposes sometimes
# import matplotlib.pyplot as plt




########################################################################
# global variables
########################################################################

data = "./data/linkages_table/"
critical_levels = "./data/linkages_table/APIS-interest-feature-critical-load-linkages_simplBB.xlsx"
levels_tab = "LINKAGES-FEATURE to CLOADS"
links = './data/linkages_table/feature_contry_v2.csv'

lpa_shape = './data/Local_Planning_Authorities/Local_Planning_Authorities_(April_2019)_UK_BUC.shp'

sac_shape = './data/Sites/SAC_BNG.shp'
spa_shape = './data/Sites/SPA_BNG.shp'
sssi_shape = './data/Sites/SSSI_BNG.shp'

substance_rename = {
    'NDEP_LEVEL': get_substance_id('ndep'),
    'SPECIESSENSITIVITYN': get_substance_id('ndep'),
    'SPECIESSENSITIVEN': get_substance_id('ndep'),
    'ACCODE': get_substance_id('adep'),
    'SPECIESSENSITIVITYA': get_substance_id('adep'),
    'SPECIESSENSITIVEA': get_substance_id('adep'),
    'NH3_CLEVEL': get_substance_id('nh3'),
    'JUSTIF_SPECIES_SENSITIVE_NH3': get_substance_id('nh3'),
    'SPECIES_SENSITIVE_NH3': get_substance_id('nh3'),
    'NOX_CLEVEL': get_substance_id('nox'),
    'JUSTIF_SPECIES_SENSITIVE_NOX': get_substance_id('nox'),
    'SPECIES_SENSITIVE_NOX': get_substance_id('nox'),
    'SO2_CLEVEL': get_substance_id('so2'),
    'JUSTIF_SPECIES_SENSITIVE_SO2': get_substance_id('so2'),
    'SPECIES_SENSITIVE_SO2': get_substance_id('so2')
}

########################################################################
# Setup functions
########################################################################


def import_feat_sens(file_name=critical_levels, tab_name=levels_tab):
    '''boilerplate for extracting a useful dataframe from the linkages
    excel file
    '''
    df = pd.read_excel(file_name, tab_name)
    # sorting by feture to create a consistent id set
    df = df.sort_values('INTERESTCODE')
    # There are duplicate rows in the linkages table
    df = df.drop_duplicates()
    # converting the string columns to string
    all_cols = list(df.columns.values)
    df[all_cols] = df[all_cols].astype(str)

    return(df)


def clean_feat(df):
    '''takes the imported features and sensitivity data and cleans it'''
    def _norm_code_col(val):
        print('_norm_code_col')
        return(unicodedata.normalize("NFKD", val))

    # getting rid of the \x0 characters
    df = df.applymap(_norm_code_col)
    # getting rid of trailing and leading spaces
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    df = df.replace("’", "", regex=True)

    return(df)


def make_feat_codes(df):
    '''takes the cleaned features data and cretes codes to use instead
    of the long descriptions of the habitat
    '''
    # There are some invalid eunis codes in the data which need to be
    # converted to out null code (000)
    df["EUNISCODE"].replace({
        "nan": 'EU000', 'D2 f': 'D2', 'information to be added': 'EU000',
        "Broad-leaved, mixed and yew woodland (Scrub - Pteridium aquilinum-\
                                        Rubus fruticosus underscrub)": 'EU000',
        "Broad-leaved, mixed and yew woodland (Scrub - Rubus fruticosus-Holcus \
                                                lanatus underscrub)": 'EU000'},
        inplace=True)

    # nan NVC codes being replaced by our null code
    df["NVC_CODE"].replace({"nan": 'NVC000'}, inplace=True)
    df["NVC_CODE_original"] = df["NVC_CODE"]

    # making a code for each of the nvc types. some of these are simple
    # nvc habiats and some are massive lists of them
    nvc_types = df['NVC_CODE'].unique().tolist()
    # we dont want to change our nvc null code
    nvc_types.remove('NVC000')
    nvc_to = [f'NVC{i}' for i in range(1, len(nvc_types))]
    nvc_rename = dict(zip(nvc_types, nvc_to))

    df["NVC_CODE"].replace(nvc_rename, inplace=True)

    # combining all the codes for the description
    df['description'] = df['HABITATCODE'] + ':' + df['NVC_CODE'] + ':'\
        + df['EUNISCODE'] + ':' + df['NCLCODE'] + ':' + df['ACCODE']

    # this is the name that we will use. the combo of name still
    # gives some duplicates so we have a numbering system
    df['name'] = df['INTERESTLAYNAME'] + ' - ' + df['INTERESTTYPE']
    df['name'] = df['name'].str.replace(' - Habitat', '')
    g = df.groupby('name')
    # adds the number next to the name
    df['name'] += g.cumcount().add(1).astype(str).radd(' ')\
        .mask(g['name'].transform('count') == 1, '')

    df = df.drop_duplicates()
    df = _create_id_col(df, 'habitat_type_id')

    return(df)


def _unstack_sens_data(df, colname):
    '''takes a df and unstacks it so that the columns are now values
    in a new col. the names are then converted to a substance id
    using a dictionary
    '''
    id_string = 'habitat_type_id'

    df.set_index(id_string, inplace=True)
    df = df.T.unstack().reset_index().rename(columns={
        'level_1': 'substance_id', 0: colname
        })

    # the substance rename converts the col names into substance
    # ids using a dictionary
    df['substance_id'] = _convert_id(df['substance_id'],
                                     substance_rename)

    return(df)


def import_sites_from_shape():
    '''importing the shapefiles for the SAC/SPA and SSSI sites'''
    df_sac = gpd.read_file(sac_shape)
    df_sac['design_status_description'] = 'SAC'
    # FIXME keeping the double country info rather than dropping it
    df_sac.drop_duplicates('SITECODE', inplace=True)
    df_spa = gpd.read_file(spa_shape)
    df_spa['design_status_description'] = 'SPA'
    df_spa.drop_duplicates('SITECODE', inplace=True)
    df_sssi = gpd.read_file(sssi_shape)
    df_sssi['design_status_description'] = 'SSSI_ASSI'
    df_sssi.drop_duplicates('SITECODE', inplace=True)

    df_all = gpd.GeoDataFrame(pd.concat([df_sac, df_spa, df_sssi],
                                        ignore_index=True))
    df_all = _create_id_col(df_all, 'assessment_area_id')

    # Converting polygons to multipolygons using shapely
    # needed to fit into the db
    df_all["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon
                          else feature for feature in df_all["geometry"]]

    return(df_all)


def get_lpa_geoms(df_sites, lpa_path=lpa_shape):
    '''maps the sites against the local planning authority
    polygons and assigns an lpa for each site. also removes the
    ones with no lpa as these are offshore
    '''
    # if the table is read from txt rather than file
    # def make_shapely(df):
    #     return(shapely.wkt.loads(df['geometry']))
    # the text geoms will need to be converted to shapely geoms
    # df_sites['geom_shape'] = df_sites.apply(lambda df: make_shapely(df),
    #                                         axis=1)

    df_lpa = gpd.read_file(lpa_path)
    lpa_tree = STRtree(df_lpa['geometry'].tolist())

    def find_lpas(df_site_r):
        # The version where the geomshape is made from text
        # site_geom = df_site_r['geom_shape']
        site_geom = df_site_r['geometry']
        find_geoms = [o for o in lpa_tree.query(site_geom)
                      if o.intersects(site_geom)]

        site_lpa_list = []
        for geo in find_geoms:
            site_lpa_list.append(
                df_lpa.loc[df_lpa.index[df_lpa['geometry'] == geo].values[0],
                           'lpa19nm'])

        # some of the sites are offshore and don't have an LPA so they
        # will error below
        if not site_lpa_list:
            return('marine')

        # TODO This is only the first in the list as the table only accepts a
        # single id
        return(site_lpa_list[0])

    df_sites['authority_id'] = df_sites.apply(lambda df: find_lpas(df), axis=1)
    # We don't want marine sites in aerius
    df_sites = df_sites[df_sites['authority_id'] != 'marine']

    df_auth = create_table_authorities(lpa_shape).data
    df_auth = df_auth[['name', 'authority_id']]
    authority_dict = _create_dictionary_from_df(df_auth)
    df_sites['authority_id'] = _convert_id(df_sites['authority_id'],
                                           authority_dict)

    return(df_sites)


def import_links(links_file=links):
    '''importing the links between site and feature'''
    df = pd.read_csv(links_file)
    # sites with no country are offshore
    df = df[df['COUNTRY'].notna()]

    return(df)


def setup_natura_tables():
    '''this sets up the natura tables - the resulting df is
    editted for the natura2000_areas and the natura2000_directive_areas
    table'''
    df_site = import_sites_from_shape()
    df_site = get_lpa_geoms(df_site)

    df_site = df_site.rename(columns={'SITECODE': 'code', 'SITENAME': 'name'})

    # the directive areas are this +1000 so we need it int
    df_site['assessment_area_id'] = df_site['assessment_area_id'].astype(int)
    df_site['natura2000_area_id'] = df_site['assessment_area_id']

    return(df_site)


########################################################################
# Extra data from NI
########################################################################

def prep_ni_data(df) -> pd.DataFrame:
    '''takes the NI shapefile and converts the habitat to the right
    format, the coord system and creates multipolygons
    '''
    # getting the total area for coverage percentages
    total_area = df['geometry'].area.sum()

    # getting rid of weird habitat names
    df = df.loc[df['Annex1'].notna()]
    # there are habitats with / or * in them. we need these though so
    # they should be changed rather than removed
    # df = df[~df["Annex1"].str.contains("/|\*")]
    df = df.replace({
        '7110*': '7110',
        '7110*/7120': '7120'
    })

    # combines the polygons into mulitpolygons
    df_n = df.dissolve(by='Annex1').reset_index()
    # Converts any remaining polygons into multipolygons with only
    # one polygon. needed for db
    df_n["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon
                        else feature for feature in df_n["geometry"]]
    # getting the areas of the habitats for the coverage
    df_n['coverage'] = df_n["geometry"].area
    df_n['coverage'] = df_n['coverage'] / total_area

    # converting the name to same format as usual for join
    df_n['INTERESTCODE'] = 'H' + df_n['Annex1']
    df_n['INTERESTTYPECODE'] = 'H'
    return(df_n)


def site_specific_geoms():
    '''This prepares the separate NI site files with specific habitat shapes.
    There is a lot of overlap here with the create table habitat areas.
    '''
    d_dir = './data/NI-habitat_maps/'
    df_b1 = gpd.read_file(d_dir + 'BallynahoneBogHabMap_FINAL.shp')
    # converting from irish grid to bng grid
    df_b1 = df_b1.to_crs(27700)
    df_b1['SITECODE'] = 'ASSI010'

    df_b2 = gpd.read_file(d_dir + 'BallynahoneBogHabMap_FINAL.shp')
    df_b2 = df_b2.to_crs(27700)
    df_b2['SITECODE'] = 'UK0016599'

    df_p1 = gpd.read_file(d_dir + 'PeatlandsParkv3_Output.shp')
    df_p1 = df_p1.to_crs(27700)
    df_p1['SITECODE'] = 'ASSI210'

    df_p2 = gpd.read_file(d_dir + 'PeatlandsParkv3_Output.shp')
    df_p2 = df_p2.to_crs(27700)
    df_p2['SITECODE'] = 'UK0030236'

    df_t = pd.concat([
        prep_ni_data(df_b1),
        prep_ni_data(df_b2),
        prep_ni_data(df_p1),
        prep_ni_data(df_p2)
        ])

    df_f = import_feat_sens()
    df_f = clean_feat(df_f)
    df_f = make_feat_codes(df_f)

    df_s = import_sites_from_shape()
    # df_s = get_lpa_geoms(df_s)

    df_full = pd.merge(
        df_t, df_f[['INTERESTCODE', 'INTERESTTYPECODE', 'habitat_type_id']],
        how='outer', on=['INTERESTCODE', 'INTERESTTYPECODE']
        )
    df_full = df_full[df_full['habitat_type_id'].notna()]
    df_full = pd.merge(
        df_full, df_s[['SITECODE', 'assessment_area_id']],
        how='inner'
        )

    return(df_full)


########################################################################
# Create table functions
########################################################################


def create_table_habitat_type_critical_levels():
    '''creates the table habitat type critical levels from the features
    excel file
    '''
    df = import_feat_sens()
    df = clean_feat(df)
    df = make_feat_codes(df)

    df['NDEP_LEVEL'] = df['CLMIN_NUTN'] + ' to ' + df['CLMAX_NUTN']

    # unstack data turns the columns into a single column and the values
    # into the next column
    df_an1 = _unstack_sens_data(
        df[['habitat_type_id', 'NDEP_LEVEL', 'ACCODE']], 'critical_level'
        )
    df_an2 = _unstack_sens_data(
        df[['habitat_type_id', 'SPECIESSENSITIVEN', 'SPECIESSENSITIVEA']],
        'sensitivity'
        )
    df_an = pd.merge(df_an1, df_an2,
                     on=['habitat_type_id', 'substance_id'], how='outer')
    df_an['result_type'] = 'deposition'

    df_nns1 = _unstack_sens_data(
        df[['habitat_type_id', 'NH3_CLEVEL', 'NOX_CLEVEL', 'SO2_CLEVEL']],
        'critical_level'
        )
    df_nns2 = _unstack_sens_data(
        df[['habitat_type_id', 'SPECIES_SENSITIVE_NH3',
            'SPECIES_SENSITIVE_NOX', 'SPECIES_SENSITIVE_SO2']],
        'sensitivity'
        )
    df_nns = pd.merge(df_nns1, df_nns2,
                      on=['habitat_type_id', 'substance_id'], how='outer')
    df_nns['result_type'] = 'concentration'

    df_t = df_an.append(df_nns).sort_values(['habitat_type_id',
                                             'substance_id'])
    df_t.reset_index(drop=True, inplace=True)

    # THIS IS THE CORRECT CODE!!!!!!!
    # just the data doesn't have the info yet for habitats
    # df_t['sensitivity'] = df_t['sensitivity'].replace({
    #     'Site specific': 't',
    #     'Yes': 't',
    #     'No': 'f'
    # })

    # FIXME temporary fix until the data it right
    df_t.loc[df_t['critical_level'] != 'nan', 'sensitivity'] = 't'
    df_t.loc[df_t['critical_level'].isin(
        ['nan', 'nan to nan']), 'sensitivity'] = 'f'

    # gets rid of the acid rows for now
    df_t = df_t[df_t['substance_id'] != get_substance_id('adep')]

    # getting the first value from the range of loads/levels
    df_t['critical_range'] = df_t['critical_level']
    df_t['critical_level'] = df_t['critical_level'].str.split(' to ').str[0]
    df_t['critical_level'] = df_t['critical_level'].str.split(' or ').str[0]

    df_t['sensitive'] = df_t['sensitivity']
    df_t['habitat_type_id'] = df_t['habitat_type_id']

    df_t = df_t.loc[~df_t['substance_id'].isin([1000, '1000'])]

    table = Table()
    table.data = df_t[[
        'habitat_type_id', 'substance_id', 'result_type',
        'critical_level', 'sensitive'
        ]]
    table.name = 'habitat_type_critical_levels'
    return(table)


def create_table_authorities(path):
    '''creates the table for the authorities from the shapefiles'''
    df_lpa = gpd.read_file(path)

    df_lpa['country_id'] = [x[0] for x in df_lpa['lpa19cd']]

    # getting the country from the code and converting to id
    countries = create_table_countries().data
    country_dict = _create_dictionary_from_df(
                                countries[['code', 'country_id']])
    df_lpa['country_id'] = _convert_id(df_lpa['country_id'],
                                       country_dict)

    df_lpa = df_lpa.rename(columns={'objectid': 'authority_id',
                                    'lpa19cd': 'code',
                                    'lpa19nm': 'name'})

    df_lpa['type'] = 'unknown'

    table = Table()
    table.data = df_lpa[['authority_id', 'country_id', 'code', 'name', 'type']]
    table.name = 'authorities'
    return(table)


def create_table_habitat_areas():
    '''creates the table for the habitat areas from the links file as well
    as the geoms from the natura table'''
    df_l = import_links()

    df_f = import_feat_sens()
    df_f = clean_feat(df_f)
    df_f = make_feat_codes(df_f)

    df_s = import_sites_from_shape()
    # the data from lpa isnt needed but this pairs the habitats down
    # to only the land sites as sea sites dont have an lpa
    df_s = get_lpa_geoms(df_s)

    df_full = pd.merge(
        df_l, df_f[['INTERESTCODE', 'INTERESTTYPECODE', 'habitat_type_id']],
        how='left', on=['INTERESTCODE', 'INTERESTTYPECODE']
        )
    df_full = df_full[df_full['habitat_type_id'].notna()]
    df_full = pd.merge(
        df_full, df_s[['SITECODE', 'assessment_area_id', 'geometry']],
        how='left'
        )
    df_full = df_full[df_full['assessment_area_id'].notna()]

    # For visual checking of the sites
    # gdf = gpd.GeoDataFrame(df_full, geometry='geometry')
    # gdf.plot()
    # plt.show()

    # Full coverage for all the habitats that dont have specific maps
    df_full['coverage'] = 1

    # These are the specific habitat geometries given by NI/Hayley
    df_ss = site_specific_geoms()

    # needs to have the site specific ones first as drop duplicates later
    # will keep the first value
    kcols = ['assessment_area_id', 'habitat_type_id', 'coverage', 'geometry']
    df_full2 = pd.concat([
        df_ss[kcols],
        df_full[kcols]
    ])
    # There are same habitats in the site specific and in the generic data
    df_full2.drop_duplicates(['assessment_area_id', 'habitat_type_id'],
                             inplace=True)
    df_full2.sort_values(['assessment_area_id', 'habitat_type_id'],
                         inplace=True)
    df_full2 = _create_id_col(df_full2, 'habitat_area_id')

    table = Table()
    table.data = df_full2[['assessment_area_id', 'habitat_area_id',
                          'habitat_type_id', 'coverage', 'geometry']]
    table.name = 'habitat_areas'
    return(table)


def create_table_natura2000_areas():
    '''just assigns the type as natura2000_area from the natura
    setup funtion
    '''
    df = setup_natura_tables()

    df['type'] = 'natura2000_area'
    # df['type'] = df['design_status_description']

    table = Table()
    table.data = df[['assessment_area_id', 'type', 'name', 'code',
                     'authority_id', 'geometry', 'natura2000_area_id']]
    table.name = 'natura2000_areas'
    return(table)


def create_table_natura2000_directives():
    '''there are only four options currently for the types of site
    and these have specific species and habitat directives'''
    # Data frame created like this to link the id with name
    # we dont want them getting mixed as more are added or taken away
    all_sub = np.array([
        ['1', 'unassigned', 'Unassigned', 'f', 'f'],
        ['2', 'SAC', 'Special Area of Conservation', 't', 'f'],
        ['3', 'SPA', 'Special Protection Area', 'f', 't'],
        ['4', 'SSSI_ASSI', 'Site of Special Scientific Interest or Area of \
                                        Special Scientific Interest', 't', 't']
        ])

    table = Table()
    table.data = pd.DataFrame(
        all_sub,
        columns=['natura2000_directive_id', 'directive_code', 'directive_name',
                 'habitat_directive', 'species_directive']
        )
    table.name = 'natura2000_directives'
    return(table)


def create_table_natura2000_directive_areas():
    df = setup_natura_tables()
    df['type'] = 'natura2000_directive_area'

    df_dir = create_table_natura2000_directives()
    df_a = pd.merge(df, df_dir.data, how='left',
                    left_on='design_status_description',
                    right_on='directive_code')

    # even though the assessment areas are the same as the areas in
    # the natura2000 table, they are treated as different areas so
    # different ids are needed
    df_a['assessment_area_id'] = df_a['assessment_area_id'] + 1000000
    df_a['natura2000_directive_area_id'] = df_a['assessment_area_id']

    table = Table()
    table.data = df_a[[
        'assessment_area_id', 'type', 'name', 'code', 'authority_id',
        'geometry', 'natura2000_directive_area_id', 'natura2000_area_id',
        'natura2000_directive_id', 'design_status_description'
        ]]
    table.name = 'natura2000_directive_areas'
    return(table)


def create_table_habitat_types():
    '''creates habitat types table from the features file'''
    df_feat = import_feat_sens()
    df_feat = clean_feat(df_feat)
    df_feat = make_feat_codes(df_feat)

    table = Table()
    table.data = df_feat[['habitat_type_id', 'name', 'description']]
    table.name = 'habitat_types'
    return(table)






