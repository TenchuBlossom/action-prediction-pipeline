import numpy as np
import tools.provider_tools as pt
import tools.py_tools as pyt
from sklearn.model_selection import train_test_split
import use_context


class Provider:

    def __init__(self, config: dict):
        self.config = config

    @staticmethod
    def __check_header_equality__(row_data, cols):
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
        to_numpy = self.config.get('to_numpy', True)
        dtype = pyt.get_dtype_instance(self.config.get('dtype', None))
        processes = self.config.get('processes', 1)
        chunksize = self.config.get('chunksize', None)

        # TODO Load in data
        with use_context.performance_profile("virtual_db"):
            virtual_db = pt.VirtualDb(dataset_dir)
            virtual_db.anchor()
            database = virtual_db.view(view_all=True, merge_partitions=True)
            virtual_dataset = pt.to_virtual_dataframe(database[y_target], y_target, preview_frac, random_seed)

        with use_context.performance_profile("train_test_split"):
            # TODO shuffle & create train test splits
            if stratify:
                train, test, _, _ = train_test_split(
                    virtual_dataset.drop([y_target], axis=1),
                    virtual_dataset[y_target],
                    test_size=test_size,
                    shuffle=shuffle,
                    stratify=virtual_dataset[y_target],
                    random_state=random_seed
                )
            else:
                train, test, _, _ = train_test_split(
                    virtual_dataset.drop([y_target], axis=1),
                    virtual_dataset[y_target],
                    test_size=test_size,
                    shuffle=shuffle,
                    random_state=random_seed
                )

        features = train.columns
        with use_context.performance_profile("compute_x_train"):
            train = virtual_db.compute(train, dtype=dtype, middleware=[self.__check_header_equality__])
            x_train = train.drop(y_names, axis=1)
            y_train = train[y_target]

        with use_context.performance_profile("compute_x_test"):
            test = virtual_db.compute(test, dtype=dtype, middleware=[self.__check_header_equality__])
            x_test = test.drop(y_names, axis=1)
            y_test = test[y_target]

        if to_numpy:
            x_train = x_train.to_numpy()
            x_test = x_test.to_numpy()
            y_train = y_train.to_numpy().flatten()
            y_test = y_test.to_numpy().flatten()
            features = features.to_numpy()

        return x_train, x_test, y_train, y_test, features







