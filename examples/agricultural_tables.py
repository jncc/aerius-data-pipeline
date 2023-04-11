import aeriusdatapipeline as adp

if __name__ == "__main__":

    output_c = 'ag_test_new_og'

    df_farm_animal_categories = adp.create_farm_animal_categories()
    df_farm_animal_categories.export_data(output_c)

    df_farm_lodging_types = adp.create_farm_lodging_types()
    df_farm_lodging_types.export_data(output_c)

    df_farm_lodging_type_emission_factor = adp.create_farm_lodging_type_emission_factor()
    df_farm_lodging_type_emission_factor.export_data(output_c)

    farm_source_categories = adp.create_farm_source_categories()
    farm_source_categories.export_data(output_c)

    farm_source_emission_factors = adp.create_farm_source_emission_factors()
    farm_source_emission_factors.export_data(output_c)