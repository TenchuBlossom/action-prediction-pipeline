import tools.file_system as fs


def compile_consumer(config: dict):

    consumer_config = config.get('consumer', None)
    if not consumer_config: return None

    consumer_name = consumer_config.get('consumer_name', None)
    if not consumer_name: return None

    module = fs.load_module(module_uri=f'consumers.{consumer_name}')
    consumer = module.__dict__['Consumer'](config)
    print('Consumer successfully compiled')
    return consumer
