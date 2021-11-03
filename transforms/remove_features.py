from custom_types.Data import State


class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, state: State):

        search_filters = self.config['filters']
        for filter_str in search_filters:
            state.data.drop(state.data.filter(regex=filter_str).columns, axis=1, inplace=True)

        return state
