import tools.file_system as fs
import tools.py_tools as pyt


def compile_consumer(config: dict):

    consumer_name = pyt.get(config, ['consumer', 'consumer_name'])
    if not consumer_name: return None

    consumer = fs.load_module(module_uri=f'consumers.{consumer_name}', class_name='Consumer', config=config)
    print('Consumer successfully compiled')
    return consumer


def compile_model(config: dict):

    model_name = pyt.get(config, ['model', 'model_name'])
    parameters = pyt.get(config, ['model', 'parameters'])

    if not model_name or not parameters: return None

    model = fs.load_module(module_uri=f'models.{model_name}', class_name='Model', config=parameters)
    print('Model successfully compiled')
    return model
