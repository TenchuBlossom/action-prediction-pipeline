from sklearn.model_selection import train_test_split
import numpy as np


class Provider:

    def __init__(self, config: dict):
        self.config = config

    def __call__(self, datasets) -> tuple:

        dataset_name = self.config['dataset_name']
        y_name = self.config['y_name']
        test_size = self.config['test_size']
        shuffle = self.config['shuffle']
        stratify = self.config['stratify']
        random_seed = self.config['random_seed']
        to_numpy = self.config['to_numpy']

        data = datasets[dataset_name]['data']
        feature_names = data.columns.tolist()

        y = data[y_name].to_frame(name=y_name)
        x = data.drop([y_name], axis=1)

        if stratify:
            x_train, x_test, y_train, y_test = train_test_split(
                x,
                y,
                test_size=test_size,
                shuffle=shuffle,
                stratify=y,
                random_state=random_seed
            )
        else:
            x_train, x_test, y_train, y_test = train_test_split(
                x,
                y,
                test_size=test_size,
                shuffle=shuffle,
                random_state=random_seed
            )

        if to_numpy:
            x_train = x_train.to_numpy()
            x_test = x_test.to_numpy()
            y_train = y_train.to_numpy()
            y_test = y_test.to_numpy()
            feature_names = np.array(feature_names)

        return x_train, x_test, y_train, y_test, feature_names
