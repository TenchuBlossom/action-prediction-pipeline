import tools.py_tools as pyt
import tools.consumer_tools as ct
import tools.file_system as fs
import use_context

class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict):

        with use_context.performance_profile(fs.filename(), "batch", "transforms"):
            default = self.config['default']
            column_types = self.config.get('columns', None)

            for _, dataset in ct.transform_gate(datasets):
                dtype_map = dict()
                for column in dataset.data.columns:

                    if column_types:
                        dtype = column_types.get(column, None)

                        if dtype:
                            dtype_map[column] = pyt.get_dtype_instance(dtype)
                            continue

                    dtype_map[column] = pyt.get_dtype_instance(default)

                dataset.data = dataset.data.astype(dtype_map)

            return datasets
