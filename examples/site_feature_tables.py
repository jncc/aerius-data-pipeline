import aeriusdatapipeline as adp

if __name__ == "__main__":

    output_c = 'aerius_data_test'

    df_feat = adp.create_table_habitat_types()
    df_feat.export_data(output_c)

    df_sens = adp.create_table_habitat_type_critical_levels()
    df_sens.export_data(output_c)

    df_areas = adp.create_table_habitat_areas()
    df_areas.export_data(output_c)

    df_nat = adp.create_table_natura2000_areas()
    df_nat.export_data(output_c)

    df_nat_dir = adp.create_table_natura2000_directive_areas()
    df_nat_dir.export_data(output_c)

    # Checking for the feats that are not in the sens table
    # pd.DataFrame({'missing_feats': df_link[df_link['habitat_type_id'].isna()]['INTERESTCODE'].unique()}).to_csv('missing_feats.csv', index=False)

    # The sites that are not found in the shapefile
    # missing_sites = list(set(df_link['SITECODE'].unique()) - set(df_site['SITECODE'].unique()))
    # print(missing_sites)
    # print(len(missing_sites))
    # df_all_sites = pd.read_csv('./data/linkages_table/sitename_code.csv', encoding = "ISO-8859-1")
    # df_all_sites = df_all_sites[df_all_sites['SITECODE'].isin(missing_sites)]
    # df_all_sites.to_csv('missing_all_sites.csv', index=False)
