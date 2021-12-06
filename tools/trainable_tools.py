import tools.py_tools as pyt
import tools.file_system as fs
from tools.constants import Constants
from tqdm import tqdm
import os
import pandas as pd

cs = Constants()


def compile_splitter(config: dict):
    splitter_name = pyt.get(config, [cs.trainable, cs.splitter, cs.name])
    parameters = pyt.get(config, [cs.trainable, cs.splitter, cs.parameters])

    if not splitter_name or not parameters: return None

    splitter = fs.load_module(module_uri=f'{cs.splitter_uri}{splitter_name}',
                              class_name=cs.Splitter, config=parameters)

    print(f'{cs.tickIcon} Splitter successfully compiled')
    return splitter


def compile_validator(config: dict, splitter):

    validator_name = pyt.get(config, [cs.trainable, cs.validator, cs.name])
    parameters = pyt.get(config, [cs.trainable, cs.validator, cs.parameters])

    if not validator_name or not parameters: return None

    diagnostics = list(pyt.get(config, [cs.diagnostics]).keys())
    if 'ConfusionMatrix' in diagnostics:
        parameters = pyt.put(parameters, True, ['return_estimator'])

    parameters = pyt.put(parameters, splitter.splitter, ['cv'])

    validator = fs.load_module(module_uri=f'{cs.validators_uri}{validator_name}',
                               class_name=cs.Validator, config=parameters)

    print(f'{cs.tickIcon} Validator successfully compiled')
    return validator


def compile_model(config: dict):
    model_name = pyt.get(config, [cs.trainable, cs.model, cs.name])
    parameters = pyt.get(config, [cs.trainable, cs.model, cs.parameters])

    if not model_name:
        raise KeyError(f"Trainable Compile Model: Model {model_name} could not be found")

    model = fs.load_module(module_uri=f'{cs.models_uri}{model_name}', class_name=cs.Model, config=parameters)
    print(f'{cs.tickIcon} Model successfully compiled')
    return model


def compile_scorers(config: dict, sklearn_compatible=False):

    scoring = pyt.get(config, [cs.trainable, cs.scorers])

    if not scoring: return None

    scorer_chain = dict()
    for module, module_name in fs.LoadPythonPackage(scoring, package_name=cs.scorers):

        if module is None: continue
        module = module.__dict__[cs.Scorer]

        if sklearn_compatible:
            module = module().compiled_scorer
        else:
            module = module().scorer

        scorer_chain[module_name] = module

    print(f'{cs.tickIcon} Scorers successfully compiled')
    return scorer_chain


def compile_diagnostics(config: dict):

    diagnostics = pyt.get(config, [cs.diagnostics])

    if not diagnostics: return None
    current_procedure = pyt.get(config, ['procedure', 'name'])

    diagnostic_chain = []
    for module, module_name in fs.LoadPythonPackage(list(diagnostics.keys()), package_name=cs.diagnostics):

        if module is None: continue
        module = module.__dict__[cs.Diagnostic]
        parameters = pyt.get(diagnostics, [module_name, 'parameters'])
        compatibility = pyt.get(parameters, ['compatibility'])

        if compatibility is not None and current_procedure not in compatibility:
            # if this diagnostic is not compatible with the current procedure then dont add it
            continue

        module = module(parameters)
        diagnostic_chain.append(module)

    print(f'{cs.tickIcon} Diagnostics successfully compiled')
    return diagnostic_chain


def execute_sync_diagnostics(results: dict, execution_chain: dict, print_txt='') -> dict:

    diagnostics_results = dict()
    for diagnostic in tqdm(execution_chain, desc=f"{cs.tickIcon} {print_txt}", colour="WHITE"):
        name = fs.get_class_filename(diagnostic)
        out = diagnostic(results)
        diagnostics_results[name] = out

    return diagnostics_results


def persist_diagnostics(location: str, diagnostic_chain: dict):
    for key, diag in diagnostic_chain.items():
        dir_path = fs.make_dir_chain(fs.path(os.path.join('../resources', location)), ['diagnostics', 'train', key])

        for item_key, item in diag.items():
            if item_key == 'figures':
                for fig_name, fig in item.items():
                    fig.savefig(os.path.join(dir_path, f'{fig_name}.png'), format='png', dpi=300)
                continue

            fs.save_python_entity(os.path.join(dir_path, f'{item_key}.diag'), item)


