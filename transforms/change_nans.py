import tools.consumer_tools as ct
from custom_types.Data import State


class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, state: State):

        value = self.config['value']
        state.data = state.data.fillna(value)

        return state
