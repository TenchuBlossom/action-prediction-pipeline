import yaml
import tools.file_system as fs
from tqdm import tqdm
from tools.constants import Constants
cs = Constants()


def compile_transforms(config: dict) -> list:

    requested_transforms = config.get(cs.transforms, None)

    if not requested_transforms: return []

    transform_chain = []
    for module, transform in fs.LoadPythonPackage(list(requested_transforms.keys()), package_name=cs.transforms):

        if module is None: continue
        module = module.__dict__[cs.Transform]
        module = module(requested_transforms[transform])
        transform_chain.append(module)

    print('Transforms successfully compiled')
    return transform_chain


def execute_transforms(transform_chain: list, dataset: dict) -> dict:
    for transform in tqdm(transform_chain, desc="Applying Transforms", colour="WHITE"):
        dataset = transform(dataset)

    return dataset


def compile_provider(config: dict):

    provider_config = config.get(cs.provider, None)
    if not provider_config: return None

    provider_name = provider_config.get(cs.name, None)
    if not provider_name: return None

    provider = fs.load_module(module_uri=f'providers.{provider_name}', class_name=cs.Provider, config=provider_config)
    print('Provider successfully compiled')
    return provider






