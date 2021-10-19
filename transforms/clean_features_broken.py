
class Transform:

    def __init__(self, config):
        self.config = config

    def __mapper__(self, col_name):

        if col_name in self.config['exceptions']:
            return col_name

        new_col_name = col_name
        isLetterA = new_col_name.startswith("A")

        if isLetterA:
            new_col_name = new_col_name[3:]

        else:
            new_col_name = new_col_name[2:]

        if new_col_name.startswith("0"):
            new_col_name = new_col_name[1:]

        return new_col_name

    def __call__(self, datasets: dict):
        for key in datasets.keys():
            dups_before_rename = any(datasets[key]['data'].columns.duplicated())
            datasets[key]['data'] = datasets[key]['data'].rename(columns=self.__mapper__)
            dup_after_rename = list(datasets[key]['data'].columns.duplicated())
            indexes = [i for i, x in enumerate(dup_after_rename) if x]
            dups = list(datasets[key]['data'].columns[indexes])


        return datasets
