import tools.py_tools as pyt
from custom_types.Data import State


class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, state: State):

        default = self.config['default']
        column_types = self.config.get('columns', None)

        dtype_map = dict()
        for column in state.data.columns:

            if column_types:
                dtype = column_types.get(column, None)

                if dtype:
                    dtype_map[column] = pyt.get_dtype_instance(dtype)
                    continue

            dtype_map[column] = pyt.get_dtype_instance(default)

        state.data = state.data.astype(dtype_map)

        return state
