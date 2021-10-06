
class Transform:

    def __init__(self, config):
        self.config = config

    def __mapper__(self, col_name):

        if col_name in self.config['exceptions']:
            return col_name

        new_col_name = col_name[2:]

        if new_col_name.startswith("0"):
            new_col_name = new_col_name[1:]

        return new_col_name

    def __call__(self, datasets: dict):
        for key in datasets.keys():
            datasets[key]['data'] = datasets[key]['data'].rename(columns=self.__mapper__)
            dup_after_rename = any(datasets[key]['data'].columns.duplicated())
            if dup_after_rename:
                raise KeyError('Clean Features Transform: Duplicate feature detected, this will cause concat errors')

        return datasets
