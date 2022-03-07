import aeriusdatapipeline as adp

if __name__ == "__main__":

    output_c = 'aerius_data_test'

    df_countries = adp.create_table_countries()
    df_countries.export_data(output_c)

    df_substances = adp.create_table_substances()
    df_substances.export_data(output_c)
