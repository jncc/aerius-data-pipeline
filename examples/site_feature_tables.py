import aeriusdatapipeline as adp
import winsound

if __name__ == "__main__":

    output_c = 'aerius_site_data_23-12_19'

    df_feat = adp.create_table_habitat_types()
    df_feat.export_data(output_c)

    df_sens,table_reason_code,table_notworking,table_reason_link,background_type = adp.create_table_habitat_type_critical_levels()
    df_sens.export_data(output_c)
    table_reason_code.export_data(output_c)
#   table_notworking.export_data(output_c)
    table_reason_link.export_data(output_c)
    background_type.export_data(output_c)        

    df_areas = adp.create_table_habitat_areas()
    df_areas.data = df_areas.data.head(500000)
    df_areas.export_data(output_c)

    df_nat = adp.create_table_natura2000_areas()
    df_nat.export_data(output_c)

    df_nat_dir = adp.create_table_natura2000_directive_areas()
    df_nat_dir.export_data(output_c)

    df_directives = adp.create_table_natura2000_directives()
    df_directives.export_data(output_c)
###
    # Checking for the feats that are not in the sens table
    # pd.DataFrame({'missing_feats': df_link[df_link['habitat_type_id'].isna()]['INTERESTCODE'].unique()}).to_csv('missing_feats.csv', index=False)

    # The sites that are not found in the shapefile
    # missing_sites = list(set(df_link['SITECODE'].unique()) - set(df_site['SITECODE'].unique()))
    # print(missing_sites)
    # print(len(missing_sites))
    # df_all_sites = pd.read_csv('./data/linkages_table/sitename_code.csv', encoding = "ISO-8859-1")
    # df_all_sites = df_all_sites[df_all_sites['SITECODE'].isin(missing_sites)]
    # df_all_sites.to_csv('missing_all_sites.csv', index=False)

    winsound.Beep(2500, 1000)
