import pandas as pd
import ray


class State:
    def __init__(self, eligible_for_transformation, batch_loader_exhausted, data=None, batch_loader_config=None,
                 length=None, metadata=None, headers=None):

        self.data = data
        self.length = length
        self.metadata = metadata
        self.batch_loader_config = batch_loader_config
        self.headers = headers
        self.eligible_for_transformation = eligible_for_transformation
        self.batch_loader_exhausted = batch_loader_exhausted


@ray.remote
class Dataset:

    def __init__(self, batch_loader_config, length=None, metadata=None):

        self.data = None
        self.batch_loader_config = batch_loader_config
        self.batch_loader = pd.read_csv(**batch_loader_config)
        self.length = length
        self.metadata = metadata
        self.headers = None
        self.eligible_for_transformation = True
        self.batch_loader_exhausted = False

    def read_data(self):
        # TODO Set data
        # TODO set headers=data.columns
        pass

    def reset(self):
        self.eligible_for_transformation = True
        self.data = None

    def spin_down(self):
        self.batch_loader_exhausted = True
        self.data = None
        self.batch_loader.close()
        self.batch_loader = None

    def get_state(self, mode='all'):

        if mode == 'all':
            return State(
                eligible_for_transformation=self.eligible_for_transformation,
                batch_loader_exhausted=self.batch_loader_exhausted,
                data=self.data,
                headers=self.headers,
                length=self.length,
                metadata=self.metadata,
            )
        elif mode == 'just_metadata':
            return State(
                eligible_for_transformation=self.eligible_for_transformation,
                batch_loader_exhausted=self.batch_loader_exhausted,
                length=self.length,
                metadata=self.metadata,
            )
        elif mode == 'just_headers':
            return State(headers=self.headers)

        elif mode == 'just_data':
            return State(data=self.data)

        elif mode == 'metadata_and_headers':
            return State(
                eligible_for_transformation=self.eligible_for_transformation,
                batch_loader_exhausted=self.batch_loader_exhausted,
                headers=self.headers,
                length=self.length,
                metadata=self.metadata,
            )

        raise ValueError(f"Dataset Error get_state(): input arg mode of {mode} is invalid. mode must be "
                         "either 'all', 'just_metadata', 'just_headers', 'just_data', 'metadata_and_headers'")