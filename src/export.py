import os


class ExportData():
    """Exporter for data structures into txt files for db.
    Input the overall output path
    """

    def __init__(self, output_path='./'):

        self.output_path = output_path

        if not self.output_path.endswith("/"):
            self.output_path = f'{self.output_path}/'

        if not os.path.exists(output_path):
            os.makedirs(output_path)

    def export_data(self, data, filename):

        data.to_csv(f'{self.output_path}{filename}.txt', sep='\t', index=False)
