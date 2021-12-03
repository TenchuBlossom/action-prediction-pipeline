import numpy as np
import tools.provider_tools as pt
from custom_types.Virtual import VirtualDb
import tools.py_tools as pyt
from sklearn.model_selection import train_test_split
import use_context
from sklearn.utils import shuffle as shuffle_data

# TODO Compute conversion to dataframe in compute function is very slow. do final operations using numpy instead only convert to
# to dataframe at the end if the user asks for it.


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

    def provide(self) -> dict:

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

        # TODO Load in data
        outputs = dict(x_train=None, x_test=None, y_train=None, y_test=None, x_all=None, y_all=None, features=None)
        with use_context.performance_profile("virtual_db"):
            virtual_db = VirtualDb(dataset_dir)
            virtual_db.anchor()
            database = virtual_db.view(view_all=True, merge_partitions=True)
            virtual_dataset = pt.to_virtual_dataframe(database[y_target], y_target, preview_frac, random_seed)

        with use_context.performance_profile("train_test_split"):
            # if test size 0 then get all data and dont do any stratifying or splitting
            if test_size <= 0:
                all_virtual_matrix = shuffle_data(virtual_dataset, random_state=random_seed, n_samples=len(virtual_dataset))
                all_virtual_matrix.drop([y_target], axis=1, inplace=True)

                # features = train_virtual_matrix.columns
                with use_context.performance_profile("compute_x_all_data"):
                    all_matrix, col_names = virtual_db.compute(all_virtual_matrix, dtype=dtype, processes=processes)
                    x_all = pyt.drop_columns_from_matrix(all_matrix, col_names, y_names)
                    y_all = pyt.keep_columns_from_matrix(all_matrix, col_names, y_target)
                    features = pyt.delete_elements_from_array(col_names, y_names)
                    outputs.update(dict(x_all=x_all, y_all=y_all, features=features))

                return outputs


            if stratify:
                train_virtual_matrix, test_virtual_matrix, _, _ = train_test_split(
                    virtual_dataset.drop([y_target], axis=1),
                    virtual_dataset[y_target],
                    test_size=test_size,
                    shuffle=shuffle,
                    stratify=virtual_dataset[y_target],
                    random_state=random_seed
                )
            else:
                train_virtual_matrix, test_virtual_matrix, _, _ = train_test_split(
                    virtual_dataset.drop([y_target], axis=1),
                    virtual_dataset[y_target],
                    test_size=test_size,
                    shuffle=shuffle,
                    random_state=random_seed
                )

        with use_context.performance_profile("compute_x_train"):
            train_matrix, col_names = virtual_db.compute(train_virtual_matrix, dtype=dtype, processes=processes)
            x_train = pyt.drop_columns_from_matrix(train_matrix, col_names, y_names)
            y_train = pyt.keep_columns_from_matrix(train_matrix, col_names, y_target)

        with use_context.performance_profile("compute_x_test"):
            test_matrix, _ = virtual_db.compute(test_virtual_matrix, dtype=dtype, processes=processes)
            x_test = pyt.drop_columns_from_matrix(test_matrix, col_names, y_names)
            y_test = pyt.keep_columns_from_matrix(test_matrix, col_names, y_target)

        outputs.update(x_train=x_train, x_test=x_test, y_train=y_train, y_test=y_test, features=col_names)
        return outputs







