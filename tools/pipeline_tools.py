import tools.file_system as fs
import tools.py_tools as pyt
from tools.constants import Constants
cs = Constants()


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


