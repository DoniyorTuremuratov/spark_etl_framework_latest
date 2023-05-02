class Config:

    def __init__(self):
        self._input_location_path = 'dataset/'
        self._output_location_path = 'C:/output'

    @property
    def input_location_path(self):
        return self._input_location_path

    @property
    def output_location_path(self):
        return self._output_location_path
