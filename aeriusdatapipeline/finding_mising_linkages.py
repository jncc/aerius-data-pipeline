import pandas as pd
from pathlib import Path

import lib_database_create as libdb

data = "./data/linkages_table/"
critical_levels = "APIS-interest-feature-critical-load-linkages_simplBB.xlsx"
levels_tab = "LINKAGES-FEATURE to CLOADS"
linkages_output = './output/link_options/'
site_names = "sitename_code.csv"
site_features = "feature_contry_v2.csv"

habitat_interests = ["Habitat (site specific)", "Habitat"]

########################################################################
# library of functions for making the feat-habitatoptions df
########################################################################


def create_feat_dictionary(data_dir=data, file_name=critical_levels,
                           tab_name=levels_tab):
    '''creating a dictionary of code to layname of the features. the
    initial datafram must have the INTEReSTCODE and INTERESTLAYNAME
    columns
    '''

    df = pd.read_excel(data_dir+file_name, tab_name)
    df = df[['INTERESTCODE', 'INTERESTLAYNAME']]
    return(libdb._create_dictionary_from_df(df))


def create_site_dictionary(data_dir=data, file_name=site_names):
    '''creating a dictionary of sitecode to site name. the
    initial datafram must have the SITECODE and SITENAME columns
    '''

    df = pd.read_csv(data_dir+file_name, encoding="ISO-8859-1")
    df = df[['SITECODE', 'SITENAME']]
    return(libdb._create_dictionary_from_df(df))


def _get_dup_feats(df):
    '''finds all the feature-feature type combos which have more than
    one possible habitat. takes a table df as argument and returns
    only the feats;types that have options'''

    df['unique_feat'] = df['INTERESTCODE']+';'+df['INTERESTTYPECODE']

    # finding all the feature codes which have multiple habitats associated
    dup_hab_feat = df[df.duplicated('unique_feat', keep=False)]
    # sorting them so they are in a consistent order if the excel changes
    dup_hab_feat = dup_hab_feat.sort_values('unique_feat')
    return(dup_hab_feat)


def _fix_habitat_strings(df):
    '''after the habitat columns have been combined, this cleans up the
    formatting issues'''

    # Some of the habitats can't be assigned, they are changed to a blank
    replacements = {
        '__': '_',
        'No critical load has not assigned for this feature, please \
            seek site specific advice': '',
        'Not Sensitive': '',
        'Designated feature/feature habitat not sensitive to eutrophi\
            cation': '',
        'No broad habitat assigned': '',
        "Species' broad habitat not sensitive to eutrophication": ""
        }
    df = df.replace(replacements, regex=True)

    # where there is no data in a column, we get extra underscores
    all_cols = list(df.columns.values)
    df[all_cols] = df[all_cols].apply(lambda x: x.str.strip('_'))

    return(df)


def _create_options_df(df):
    '''takes in a df with the feat;types that have options and returns
    a df with the options listed as the columns with each feat'type
    as an individual row'''

    # this is a list of the feats with additional habitats
    # needs to be global as will be used later to match with links
    unique_feats = list(df['unique_feat'].unique())
    # this is the max number of habitats associated with one feat
    max_options_n = df['unique_feat'].value_counts().max()

    feat_name_dict = create_feat_dictionary()

    feat_total_list = []
    # loop throug the feats and create a list of each of the potential
    # habitats for that feat
    for feat in unique_feats:
        # filling in the first few columns, split the unique feat_feattype
        # and convert into proper names
        feat_list = [feat, feat_name_dict[feat.split(';')[0]]]
        # if I want to include the type, reconbine with the feat_name_dict
        # feat_list = [feat_name,
        #              feat_name_dict[feat_name]+';'+feat.split(';')[1]]

        df_feat = df[df['unique_feat'] == feat]
        feat_list = feat_list + (list(df_feat['habitat']))
        # if there are different numbers of habs for each feat, this will
        # even it out so it can later be turned into a df
        feat_list += [''] * (max_options_n - len(feat_list))
        feat_total_list.append(feat_list)

    df_options = pd.DataFrame(feat_total_list)
    # renaming first column so it can later be merged
    df_options = df_options.rename(columns={0: 'unique_feat',
                                            1: 'feat_name'})
    return(df_options)


########################################################################
# creating the feat-habitatoptions dfs
########################################################################

# dup_feat['habitat'] = dup_feat['habitat_id']\
#                         +'/'+dup_feat['HABITATCODE']\
#                         +'_'+dup_feat['NVC_CODE']\
#                         +'_'+dup_feat['NCLCODE']\
#                         +'_'+dup_feat['ACCODE']

def _get_feat_df_from_excel(
        data_dir=data, file_name=critical_levels, tab_name=levels_tab
        ):
    '''boilerplate for extracting a useful dataframe from the linkages
    excel file
    '''

    df = pd.read_excel(data_dir+file_name, tab_name)
    # sorting by feture to create a consistent id set
    df = df.sort_values('INTERESTCODE')
    # There are duplicate rows in the linkages table
    df = df.drop_duplicates()
    # converting the string columns to string
    all_cols = list(df.columns.values)
    df[all_cols] = df[all_cols].astype(str)


def _subdf_habitat(df):
    return(df[df['INTERESTTYPE'].isin(habitat_interests)])


def _subdf_species(df):
    return(df[-df['INTERESTTYPE'].isin(habitat_interests)])


def create_hab_options_df():
    '''creates the options list for habitat features'''

    # grabbing the habitat_types table from create tables lib
    df_feat = _get_feat_df_from_excel()
    df_feat = _subdf_habitat(df_feat)
    df_feat = libdb._create_id_col(df_feat, 'habitat_id')

    dup_feat = _get_dup_feats(df_feat)

    # Creating the identifying habitat column, has the id at the beginning
    # as a reference point
    dup_feat['habitat'] = dup_feat['habitat_id']\
        + '/'+dup_feat['BROADHABITAT']\
        + '___NVC-'+dup_feat['NVC_CODE']\
        + '___Eunis-'+dup_feat['EUNISCODE']\
        + '___NitrogenClass-'+dup_feat['NCLCLASS']\
        + '___AcidityClass-'+dup_feat['ACIDITYCLASS']

    dup_feat = _fix_habitat_strings(dup_feat)

    df_options = _create_options_df(dup_feat)

    return(df_options)


def create_species_options_df():
    '''creates the options list for species features'''

    # grabbing the habitat_types table from create tables lib
    df_feat = _get_feat_df_from_excel()
    df_feat = _subdf_species(df_feat)
    df_feat = libdb._create_id_col(df_feat, 'habitat_id')

    dup_feat = _get_dup_feats(df_feat)

    # Creating the identifying habitat column, has the id at the beginning
    # as a reference point
    dup_feat['habitat'] = dup_feat['habitat_id']\
        + '/'+dup_feat['BROADHABITAT']\
        + '___NitrogenClass-'+dup_feat['NCLCLASS']\
        + '___AcidityClass-'+dup_feat['ACIDITYCLASS']

    dup_feat = _fix_habitat_strings(dup_feat)

    df_options = _create_options_df(dup_feat)

    return(df_options)


########################################################################
# appending the linkages df
########################################################################


def linkages_with_options(feat_func):
    '''takes the feat;type with options dataframe and matches it with
    the sites that have that feat;type
    '''

    # habitat or species depending on what we need
    df = feat_func()

    # the file location comes from lib
    df_links = pd.read_csv(data+site_features)

    # getting the unique eats to pair down the linksages table
    unique_feats = list(df['unique_feat'])
    # combining the featcode and feattypecode to create like unique_feats
    df_links['unique_feat'] = \
        df_links['INTERESTCODE']+';'+df_links['INTEREST_TYPE_CODE']
    # selecting only the site-feat links for feats that have options
    df_links = df_links[df_links['unique_feat'].isin(unique_feats)]

    df_options_final = pd.merge(df_links, df, on='unique_feat')
    df_options_final = df_options_final.drop([
        'INTERESTCODE', 'INTEREST_TYPE_CODE', 'unique_feat'
        ], axis=1)

    site_name_dict = create_site_dictionary()

    # changing site codes to site names with a dictionary created at top
    df_options_final['SITECODE'] = \
        df_options_final['SITECODE'].replace(site_name_dict)

    df_options_final = df_options_final.sort_values(['COUNTRY', 'SITECODE'])

    return(df_options_final)


def hab_linkages_with_options(create_hab_options_df):
    return(linkages_with_options(create_hab_options_df))


def spec_linkages_with_options(create_species_options_df):
    return(linkages_with_options(create_species_options_df))


########################################################################
# running the code to get linkages + options
########################################################################

if __name__ == "__main__":

    Path(linkages_output).mkdir(parents=True, exist_ok=True)

    df_options_spec = create_species_options_df()
    # df_options_spec.to_csv(linkages_output+'species_options.csv', index=False)
    df_options_final = spec_linkages_with_options()
    df_options_final[df_options_final['COUNTRY'].isin(['Scotland', 'England/Scotland'])].to_csv(linkages_output+'scotland_species_options.csv', index=False)
    df_options_final[df_options_final['COUNTRY'].isin(['Wales', 'Wales/Scotland'])].to_csv(linkages_output+'wales_species_options.csv', index=False)
    df_options_final[df_options_final['COUNTRY'].isin(['N Ireland'])].to_csv(linkages_output+'northern_ireland_species_options.csv', index=False)
    df_options_final[df_options_final['COUNTRY'].isin(['England'])].to_csv(linkages_output+'england_species_options.csv', index=False)

    df_options_hab = create_hab_options_df()
    # df_options_hab.to_csv(linkages_output+'habitat_options.csv', index=False)
    df_options_final = hab_linkages_with_options()
    df_options_final[df_options_final['COUNTRY'].isin(['Scotland', 'England/Scotland'])].to_csv(linkages_output+'scotland_habitat_options.csv', index=False)
    df_options_final[df_options_final['COUNTRY'].isin(['Wales', 'Wales/Scotland'])].to_csv(linkages_output+'wales_habitat_options.csv', index=False)
    df_options_final[df_options_final['COUNTRY'].isin(['N Ireland'])].to_csv(linkages_output+'northern_ireland_options.csv', index=False)
    # df_options_final[df_options_final['COUNTRY'].isin(['England'])].to_csv(linkages_output+'england_habitat_options.csv', index=False)
