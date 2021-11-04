import tools.py_tools as pyt


def init_dataset_resources(config: dict, access_path: list):
    defaults = {
        'num_cpus': 1
    }

    if config is None: return defaults

    return pyt.get(config, access_path, defaults)