import numpy as np
import tools.provider_tools as pt
import tools.py_tools as pyt
from mergedeep import merge
from sklearn.model_selection import train_test_split


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
        preview_frac = self.config.get('preview_frac', 1.0)
        y_names = self.config['y_names']
        y_target = self.config['y_target']
        test_size = self.config['test_size']
        shuffle = self.config['shuffle']
        stratify = self.config['stratify']
        random_seed = self.config.get('random_seed', None)
        dtype = pyt.get_dtype_instance(self.config.get('dtype', None))

        # TODO Load in data
        virtual_db = pt.VirtualDb(dataset_dir)
        virtual_db.anchor()
        database = virtual_db.view(view_all=True, merge_partitions=True)
        virtual_dataset = pt.to_virtual_dataframe(database[y_target], y_target, preview_frac, random_seed)

        # TODO shuffle & create train test splits
        if stratify:
            x_train, x_test, y_train, y_test = train_test_split(
                virtual_dataset.drop([y_target], axis=1),
                virtual_dataset[y_target],
                test_size=test_size,
                shuffle=shuffle,
                stratify=virtual_dataset[y_target],
                random_state=random_seed
            )
        else:
            x_train, x_test, y_train, y_test = train_test_split(
                virtual_dataset.drop([y_target], axis=1),
                virtual_dataset[y_target],
                test_size=test_size,
                shuffle=shuffle,
                random_state=random_seed
            )

        x_train = virtual_db.compute(x_train, dtype=dtype, middleware=[self.__check_header_equality__])
        x_train = x_train.drop(y_names, axis=1)

        x_test = virtual_db.compute(x_test, dtype=dtype, middleware=[self.__check_header_equality__])
        x_test = x_test.drop(y_names, axis=1)

        # feature_names = dataset.columns

        x_train = x_train.to_numpy()
        x_test = x_test.to_numpy()
        y_train = y_train.to_numpy().flatten()
        y_test = y_test.to_numpy().flatten()
        feature_names = feature_names.to_numpy()
        return x_train, x_test, y_train, y_test, feature_names







