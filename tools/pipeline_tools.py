import tools.file_system as fs
import tools.py_tools as pyt
from tools.constants import Constants
cs = Constants()


def compile_consumer(config: dict):

    consumer_name = pyt.get(config, [cs.consumer, cs.name])
    if not consumer_name: return None

    consumer = fs.load_module(module_uri=f'{cs.consumers_uri}{consumer_name}', class_name=cs.Consumer, config=config)
    print('Consumer successfully compiled')
    return consumer


def compile_trainable(config: dict):

    trainable_name = pyt.get(config, [cs.trainable, cs.name])

    if not trainable_name: return None

    trainable = fs.load_module(module_uri=f'{cs.trainables_uri}{trainable_name}',
                               class_name=cs.Trainable, config=config)

    print('Trainable successfully compiled')
    return trainable

