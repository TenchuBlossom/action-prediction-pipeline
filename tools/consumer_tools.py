import yaml
import tools.file_system as fs
from tqdm import tqdm
from tools.constants import Constants
import tools.pipeline_tools as pt

import pandas as pd
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

    print(f'{cs.tickIcon} Transforms successfully compiled')
    return transform_chain


def execute_transforms(transform_chain: list, datasets: dict) -> dict:
    for transform in tqdm(transform_chain, desc="Applying Transforms", colour="WHITE"):
        datasets = transform(datasets)

    return datasets


def reset_datasets(datasets: dict):
    for _, dataset in datasets:
        dataset.reset()
    return datasets


def compile_provider(config: dict):

    provider_config = config.get(cs.provider, None)
    if not provider_config: return None

    provider_name = provider_config.get(cs.name, None)
    if not provider_name: return None

    provider = fs.load_module(module_uri=f'providers.{provider_name}', class_name=cs.Provider, config=provider_config)
    print(f'{cs.tickIcon} Provider successfully compiled')
    return provider


def transform_gate(datasets: dict, ignore_gate=False, dummy_exhausted_datasets=False):

    if ignore_gate: return datasets.items()

    gated_datasets = []
    for key, dataset in datasets.items():

        if not dataset.eligible_for_transformation: continue

        if dataset.batch_loader_exhausted:
            if dummy_exhausted_datasets:
                dataset.data = pd.DataFrame(None, columns=dataset.headers)
                gated_datasets.append((key, dataset))

            continue

        gated_datasets.append((key, dataset))

    return gated_datasets







