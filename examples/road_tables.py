import winsound

import aeriusdatapipeline as adp

if __name__ == "__main__":

    output_c = 'aerius_road_data_23-12-19'

    df_area = adp.create_table_road_area_categories()
    df_area.export_data(output_c)
    df_types = adp.create_table_road_type_categories()
    df_types.export_data(output_c)
    df_vehicle = adp.create_table_road_vehicle_categories()
    df_vehicle.export_data(output_c)
    df_speed = adp.create_table_road_speed_profiles()
    df_speed.export_data(output_c)
    
    df_area_to_type = adp.create_table_road_areas_to_road_types()
    df_area_to_type.export_data(output_c)
    df_type_to_speed = adp.create_table_road_types_to_speed_profiles()
    df_type_to_speed.export_data(output_c)
    df_cat = adp.create_table_road_categories()
    df_cat.export_data(output_c)
    
    df_emission = adp.create_table_road_category_emission_factors()
    df_emission.export_data(output_c)

    years=[2018,2019,2020,2021,2022]

    standard_diurnal_variation_profiles = adp.create_table_standard_diurnal_variation_profiles(years)
    standard_diurnal_variation_profiles.export_data(output_c)

    standard_diurnal_variation_profiles_values = adp.create_table_standard_diurnal_variation_profiles_values(years)
    standard_diurnal_variation_profiles_values.export_data(output_c)

    sector_default_diurnal_variation_profiles  = adp.create_table_sector_default_diurnal_variation_profiles()
    sector_default_diurnal_variation_profiles.export_data(output_c)

    sector_year_default_diurnal_variation_profiles = adp.create_table_sector_year_default_diurnal_variation_profiles(years)
    sector_year_default_diurnal_variation_profiles.export_data(output_c)

    winsound.Beep(2500, 1000)
