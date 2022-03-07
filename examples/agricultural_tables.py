import aeriusdatapipeline as adp

if __name__ == "__main__":

    output_c = 'aerius_data_test'

    df_animal = adp.create_table_farm_animal_categories()
    df_animal.export_data(output_c)

    df_lodging_types = adp.create_table_farm_lodging_types()
    df_lodging_types.export_data(output_c)

    df_mitigation_types = adp.create_table_farm_reductive_lodging_system()
    df_mitigation_types.export_data(output_c)

    lodging_emissions = adp.create_table_farm_lodging_type_emission_factors()
    lodging_emissions.export_data(output_c)

    lodging_reductions = adp.create_table_farm_reductive_lodging_system_reduction_factor()
    lodging_reductions.export_data(output_c)

    farm_links = adp.create_table_farm_lodging_types_to_reductive_lodging_systems()
    farm_links.export_data(output_c)
