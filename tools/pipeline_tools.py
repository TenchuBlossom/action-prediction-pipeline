import tools.file_system as fs
import tools.py_tools as pyt
from tools.constants import Constants
import os
cs = Constants()


def compile_data_config(config: dict, data_config_src: str):
    data_config = fs.compile_config(data_config_src)
    data_config = pyt.put(data_config, config['performance_profile'], ['consumer', 'performance_profile'])
    data_config = pyt.put(data_config, config['performance_profile'], ['provider', 'performance_profile'])
    return data_config


def compile_train_config(config: dict, trainable_config_src: dict):
    trainable_config = fs.compile_config(trainable_config_src)
    trainable_config = pyt.put(trainable_config, config['performance_profile'], ['validator', 'performance_profile'])
    trainable_config = pyt.put(trainable_config, config['performance_profile'], ['model', 'performance_profile'])
    trainable_config = pyt.put(trainable_config, config['performance_profile'], ['diagnostics', 'performance_profile'])
    return trainable_config


def compile_consumer(config: dict):

    consumer_name = pyt.get(config, [cs.consumer, cs.name])
    if not consumer_name: return None

    consumer = fs.load_module(module_uri=f'{cs.consumers_uri}{consumer_name}', class_name=cs.Consumer, config=config)
    print(f'{cs.tickIcon} Consumer successfully compiled')
    return consumer


def compile_trainable(config: dict):

    trainable_name = pyt.get(config, [cs.trainable, cs.name])

    if not trainable_name: return None

    trainable = fs.load_module(module_uri=f'{cs.trainables_uri}{trainable_name}',
                               class_name=cs.Trainable, config=config)

    print('Trainable successfully compiled')
    return trainable


def compile_provider(config: dict):

    provider_config = config.get(cs.provider, None)
    if not provider_config: return None

    provider_name = provider_config.get(cs.name, None)
    if not provider_name: return None

    provider = fs.load_module(module_uri=f'providers.{provider_name}', class_name=cs.Provider, config=provider_config)
    print(f'{cs.tickIcon} Provider successfully compiled')
    return provider


class Dataset:

    def __init__(self, data=None, batch_loader=None, length=None, metadata=None, src=None):

        self.data = data
        self.batch_loader = batch_loader
        self.length = length
        self.metadata = metadata
        self.src = src
        self.headers = data.columns if data is not None else None
        self.eligible_for_transformation = True
        self.batch_loader_exhausted = False

    def reset(self):
        self.eligible_for_transformation = True
        self.data = None

    def spin_down(self):
        self.batch_loader_exhausted = True
        self.data = None
        self.batch_loader.close()
        self.batch_loader = None


