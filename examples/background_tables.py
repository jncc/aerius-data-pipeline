import winsound

import aeriusdatapipeline as adp

if __name__ == "__main__":

    output_c = 'aerius_data_test'

    df_cell_table = adp.create_table_background_cell()
    df_cell_table.export_data(output_c)

    df_conc = adp.create_table_background_concentration()
    df_conc.export_data(output_c)

    df_dep = adp.create_table_background_cell_depositions()
    df_dep.export_data(output_c)

    winsound.Beep(2500, 100)
