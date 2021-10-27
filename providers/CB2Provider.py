from sklearn.model_selection import train_test_split
import numpy as np
import tools.provider_tools as pt
import tools.py_tools as pyt


class Provider:

    def __init__(self, config: dict):
        self.config = config

    def __check_header_equality__(self, row_data, cols):
        if cols is None: return row_data
        row_headers = row_data['features'].values
        if not np.array_equal(row_headers, cols):
            raise ValueError('Provider check header equality: Provider detected a that a parition has different'
                             'headers to the current columns ')

        return row_data

    def provide(self) -> tuple:

        dataset_dir = self.config['dataset_dir']
        preview_frac = self.config['preview_frac']
        y_names = self.config['y_names']
        y_target = self.config['y_target']
        test_size = self.config['test_size']
        shuffle = self.config['shuffle']
        stratify = self.config['stratify']
        random_seed = self.config['random_seed']
        dtype = pyt.get_dtype_instance(self.config.get('dtype', None))

        # TODO Load in data
        virtual_db = pt.VirtualDb(dataset_dir)
        virtual_db.anchor()
        database = virtual_db.view(view_all=True, merge_partitions=True)
        compute_params = database['CB2_CAMP']['0.0']
        values = virtual_db.compute(
            compute_params['index'],
            compute_params['partition'],
            dtype=dtype,
            middleware=[self.__check_header_equality__]
        )
        a = 0







