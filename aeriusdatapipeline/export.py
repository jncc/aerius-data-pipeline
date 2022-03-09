from pathlib import Path


class ExportData():
    """Exporter for data structures into txt files for db.
    Input the overall output path
    """

    def export_data(self, filepath):
        root = f'./output/{filepath}/'
        print(f'Exporting file {self.name} to {root}\n')
        Path(root).mkdir(parents=True, exist_ok=True)
        self.data.to_csv(f'{root}/{self.name}.txt', sep='\t', index=False)


class Table():
    """Exporter for data structures into txt files for db.
    Input the overall output path
    """

    def __init__(self):

        self.name = None
        self.data = None

    def export_data(self, filepath):

        # errors for missing name and data

        root = f'./output/{filepath}/'
        print(f'Exporting file {self.name} to {root}\n')
        Path(root).mkdir(parents=True, exist_ok=True)
        self.data.to_csv(f'{root}/{self.name}.txt', sep='\t', index=False)
