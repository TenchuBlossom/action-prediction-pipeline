import numpy as np
import tools.consumer_tools as ct
import tools.file_system as fs
from custom_types.Data import State
import use_context


class Transform:

    def __init__(self, config):
        self.config = config

    @staticmethod
    def __mapper__(element, possible_values: list, relabels: dict):

        if element in possible_values:
            return element

        new_element = relabels.get(element, None)
        if new_element:
            return new_element

        return np.nan

    def __call__(self, state: State):

        labels = self.config['labels']

        for column, label in labels.items():
            possible_values = label['possible_values']
            relabels = label['relabels']
            state.data[column] = state.data[column].apply(self.__mapper__, args=[possible_values, relabels])

        state.data.dropna(subset=list(labels.keys()), inplace=True)

        return state
