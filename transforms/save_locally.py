import tools.consumer_tools as ct
import tools.file_system as fs
import tools.py_tools as pyt
import use_context
import os


class Transform:

    def __init__(self, config):
        self.config = config
        self.index = 0
        self.partition = 0

        dir_location = pyt.get(config, ['dir_location'])
        dir_name = pyt.get(config, ['dir_name'])
        self.dir_pathname = fs.path(os.path.join(dir_location, dir_name))
        fs.make_dir(self.dir_pathname)

    def __to_csv__(self, row, pathname, virtual_db):
        row.to_csv(os.path.join(pathname, f'{self.index}_row.csv'), index=True)

        partition_on_columns = pyt.get(self.config, ['partition_on_columns'])
        for col_name in partition_on_columns:
            val = row[col_name]

            key_exist = virtual_db.get(col_name, None)
            if key_exist is None: virtual_db[col_name] = dict()

            key_exist = virtual_db[col_name].get(val, None)
            if key_exist is None: virtual_db[col_name][val] = dict(index=[], partition=[], byte_size=[])

            virtual_db[col_name][val]['index'].append(self.index)
            virtual_db[col_name][val]['partition'].append(f'{self.partition}_partition')
            virtual_db[col_name][val]['byte_size'].append(row.memory_usage())

        self.index += 1

    def __call__(self, datasets: dict):

        with use_context.performance_profile(fs.filename(), "batch", "transforms"):
            for name, dataset in ct.transform_gate(datasets):
                # TODO create dataset folders & create partition folder
                root_pathname = fs.make_dir(os.path.join(self.dir_pathname, name))
                partition_pathname = fs.make_dir(os.path.join(root_pathname, f'{self.partition}_partition'))
                row_pathname = fs.make_dir(os.path.join(partition_pathname, 'rows'))
                dataset.data.columns\
                    .to_series()\
                    .to_csv(os.path.join(partition_pathname, 'headers.csv'), index=False, sep='\t')
                virtual_db = dict()
                dataset.data.apply(self.__to_csv__, args=[row_pathname, virtual_db], axis=1)

                # TODO save virtual database as json to partition
                fs.save_json(os.path.join(partition_pathname, 'virtual_db.json'), virtual_db)

            self.partition += 1
            return datasets


