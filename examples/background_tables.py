import winsound
from pathlib import Path
import aeriusdatapipeline as adp

if __name__ == "__main__":

    output_c = 'aerius_data_background_tables_20240105'

    root = f'./output/{output_c}/'
    Path(root).mkdir(parents=True, exist_ok=True)

    df_back_years,deposition_type=adp.create_background_years_table()
    df_back_years.export_data(output_c)
    deposition_type.export_data(output_c)

    df_target_back_years=adp.create_target_background_years_table()
    df_target_back_years.export_data(output_c)

    print('Starting df_cell' )
    df_cell=adp.create_background_cell(output_c)
    df_cell.export_data(output_c)

    print('Starting df_conc' )
    df_conc=adp.create_background_concentration(output_c)
    df_conc.export_data(output_c)

    print('Starting df_dep' )
    df_dep=adp.create_background_cell_depositions(output_c)
    df_dep.export_data(output_c)

    print('Starting background_jurisdiction_policies' )
    background_jurisdiction_policies=adp.create_background_jurisdiction_policies(output_c)
    background_jurisdiction_policies.export_data(output_c)
    winsound.Beep(2500, 100)
