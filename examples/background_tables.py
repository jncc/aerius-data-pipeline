import winsound

import aeriusdatapipeline as adp

if __name__ == "__main__":

    output_c = 'aerius_data_background_tables_20230221'

    df_back_years=adp.create_background_years_table()
    df_back_years.export_data(output_c)

    df_target_back_years=adp.create_target_background_years_table()
    df_target_back_years.export_data(output_c)

    df_cell=adp.create_background_cell()
    df_cell.export_data(output_c)

    df_conc=adp.create_background_concentration()
    df_conc.export_data(output_c)

    df_dep=adp.create_background_cell_depositions()
    df_dep.export_data(output_c)

    background_jurisdiction_policies=adp.create_background_jurisdiction_policies()

    background_jurisdiction_policies.export_data(output_c)
    winsound.Beep(2500, 100)
