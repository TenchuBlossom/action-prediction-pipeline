import tools.file_system as fs
from tqdm import tqdm
from tools.constants import Constants
import ray
from collections import OrderedDict, Callable
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


def transform_gate(datasets: dict, ignore_gate=False, dummy_exhausted_datasets=False):

    if ignore_gate: return datasets.items()

    gated_datasets = OrderedDict()

    for key, dataset in datasets.items():
        state = ray.get(dataset.get_state.remote(mode='just_metadata'))
        if not state.eligible_for_transformation: continue

        if state.batch_loader_exhausted:
            if dummy_exhausted_datasets:
                worker_id = dataset.init_dummy_data.remote()
                ray.wait([worker_id], timeout=60.0)
                gated_datasets[key] = dataset

            continue

        gated_datasets[key] = dataset

    return gated_datasets


def get_transform_params(transform):

    dummy_exhausted_datasets = False
    sync_process = False
    ignore_gate = False

    if hasattr(transform, 'dummy_exhausted_datasets'):
        dummy_exhausted_datasets = transform.dummy_exhausted_datasets

    elif transform.config.get('dummy_exhausted_datasets', None) is not None:
        dummy_exhausted_datasets = transform.config['dummy_exhausted_datasets']

    if hasattr(transform, 'sync_process'):
        sync_process = True

    elif transform.config.get('sync_process', None) is not None:
        sync_process = transform.config['sync_process']

    if hasattr(transform, 'ignore_gate'):
        ignore_gate = transform.ignore_gate

    elif transform.config.get('ignore_gate', None) is not None:
        ignore_gate = transform.config['ignore_gate']

    return dummy_exhausted_datasets, sync_process, ignore_gate








