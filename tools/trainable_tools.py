import tools.py_tools as pyt
import tools.file_system as fs
from tools.constants import Constants

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

    if 'ConfusionMatrix' in pyt.get(config, [cs.diagnostics]):
        parameters = pyt.put(parameters, True, ['return_estimator'])

    parameters = pyt.put(parameters, splitter.splitter, ['cv'])
    parameters = pyt.put(parameters, compile_scorers(config), ['scoring'])

    validator = fs.load_module(module_uri=f'{cs.validators_uri}{validator_name}',
                               class_name=cs.Validator, config=parameters)

    print(f'{cs.tickIcon} Validator successfully compiled')
    return validator


def compile_model(config: dict):
    model_name = pyt.get(config, [cs.trainable, cs.model, cs.name])
    parameters = pyt.get(config, [cs.trainable, cs.model, cs.parameters])

    if not model_name or not parameters: return None

    model = fs.load_module(module_uri=f'{cs.models_uri}{model_name}', class_name=cs.Model, config=parameters)
    print(f'{cs.tickIcon} Model successfully compiled')
    return model


def compile_scorers(config: dict):

    scoring = pyt.get(config, [cs.trainable, cs.validator, cs.parameters, 'scoring'])

    if not scoring: return []

    scorer_chain = dict()
    for module, transform in fs.LoadPythonPackage(scoring, package_name=cs.scorers):

        if module is None: continue
        module = module.__dict__[cs.Scorer]
        module = module().scorer
        scorer_chain[transform] = module

    print(f'{cs.tickIcon} Scorers successfully compiled')
    return scorer_chain
