import pandas as pd
import ray
from collections.abc import Callable
import tools.file_system as fs
from collections import OrderedDict
import tools.py_tools as pyt
import os
import ray
import asyncio


def select(new_val, old_val):
    return new_val if new_val is not None else old_val


class State:
    def __init__(self, name=None, eligible_for_transformation=None, batch_loader_exhausted=None, data=None,
                 batch_loader_config=None, length=None, chunk_length=None, metadata=None, headers=None):

        self.name = name
        self.data = data
        self.length = length
        self.chunk_length = chunk_length
        self.metadata = metadata
        self.batch_loader_config = batch_loader_config
        self.eligible_for_transformation = eligible_for_transformation
        self.batch_loader_exhausted = batch_loader_exhausted
        self.headers = headers


@ray.remote
class Dataset:

    def __init__(self, name=None, batch_loader_config=None, data=None, length=None, metadata=None):

        self.name = name
        self.data = None
        self.batch_loader_config = batch_loader_config
        self.batch_loader = pd.read_csv(**batch_loader_config) if batch_loader_config is not None else None
        self.length = length
        self.chunk_length = len(data) if data is not None else None
        self.metadata = metadata
        self.headers = data.columns if data is not None else None
        self.eligible_for_transformation = True
        self.batch_loader_exhausted = False

    def read_data(self):
        try:
            chunk = next(self.batch_loader)
            self.headers = chunk.columns
            self.chunk_length = len(chunk)
            self.data = chunk

        except StopIteration:
            self.spin_down()

    def reset(self):
        self.eligible_for_transformation = True
        self.data = None

    def spin_down(self):
        self.batch_loader_exhausted = True
        self.data = None
        self.batch_loader.close()
        self.batch_loader = None
        self.chunk_length = 0

    def terminate(self):
        ray.actor.exit_actor()

    def init_dummy_data(self):
        self.data = pd.DataFrame(None, columns=self.headers)

    def transform(self, transform: Callable):
        new_state = transform(self.get_state())
        self.update_state(new_state)

    def update_state(self, state: State):
        self.name = select(state.name, self.name)
        self.data = select(state.data, self.data)
        self.batch_loader_config = select(state.batch_loader_config, self.batch_loader_config)
        self.length = select(state.length, self.length)
        self.chunk_length = select(state.chunk_length, self.chunk_length)
        self.metadata = select(state.metadata, self.metadata)
        self.headers = self.data.columns
        self.eligible_for_transformation = select(state.eligible_for_transformation, self.eligible_for_transformation)
        self.batch_loader_exhausted = select(state.batch_loader_exhausted, self.batch_loader_exhausted)

    def merge_actor_data(self, actors: list, name=None):
        states = ray.get([actor.get_state.remote(mode='just_data') for actor in actors])
        data_to_concat = [state.data for state in states]
        new_data = pd.concat(data_to_concat)
        new_state = State(
            name=name,
            data=new_data,
            chunk_length=len(new_data),
        )
        self.update_state(new_state)

    def get_state(self, mode='all'):

        if mode == 'all':
            return State(
                eligible_for_transformation=self.eligible_for_transformation,
                batch_loader_exhausted=self.batch_loader_exhausted,
                data=self.data,
                headers=self.headers,
                length=self.length,
                chunk_length=self.chunk_length,
                metadata=self.metadata,
                batch_loader_config=self.batch_loader_config,
                name=self.name
            )
        elif mode == 'just_metadata':
            return State(
                eligible_for_transformation=self.eligible_for_transformation,
                batch_loader_exhausted=self.batch_loader_exhausted,
                length=self.length,
                metadata=self.metadata,
                chunk_length=self.chunk_length,
                batch_loader_config=self.batch_loader_config,
                name=self.name
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
                chunk_length=self.chunk_length,
                batch_loader_config=self.batch_loader_config,
                name=self.name
            )

        raise ValueError(f"Dataset Error get_state(): input arg mode of {mode} is invalid. mode must be "
                         "either 'all', 'just_metadata', 'just_headers', 'just_data', 'metadata_and_headers'")


