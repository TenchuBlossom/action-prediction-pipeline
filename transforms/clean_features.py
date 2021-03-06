import tools.consumer_tools as ct
from custom_types.Data import State
import tools.file_system as fs
import use_context


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

    def __call__(self, state: State):

        check_for_dups = self.config.get('check_for_duplicates', False)

        state.data = state.data.rename(columns=self.__mapper__)

        if not check_for_dups: return state

        dup_after_rename = any(state.data.columns.duplicated())
        if dup_after_rename:
            raise KeyError('Clean Features Transform: Duplicate feature detected, this will cause concat errors')

        return state
