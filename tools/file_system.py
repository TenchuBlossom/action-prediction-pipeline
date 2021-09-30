import os
import importlib
import yaml
from tools.constants import Constants
import inspect
cs = Constants()


def path(pathname: str, root_depth=2) -> str:

    pathname = pathname.replace('../', '')

    MAIN_DIR = __file__
    for i in range(root_depth):
        MAIN_DIR = os.path.dirname(MAIN_DIR)

    f_path = os.path.join(MAIN_DIR, pathname)
    return f_path


def path_exists(pathname, throw_exception=False) -> bool:

    if not os.path.exists(pathname):
        if throw_exception:
            raise FileExistsError(f'File Does not exist: {pathname} try using tools.path')
        return False

    return True


def make_dir(pathname: str) -> tuple:
    if not os.path.exists(pathname):
        os.mkdir(pathname)
        return True, pathname

    return False, pathname


def make_dir_chain(pathname: str, dir_chain: list) -> tuple:

    for d in dir_chain:
        _, pathname = make_dir(os.path.join(pathname, d))

    return pathname


def get_dirs(pathname: str) -> tuple:

    pathname = path(pathname)
    dirs = []
    full_path_dirs = []
    for file in os.listdir(pathname):
        if os.path.isdir(os.path.join(pathname, file)):
            dirs.append(file)
            full_path_dirs.append(os.path.join(pathname, file))

    return dirs, full_path_dirs


def find_file_type(pathname: str, file_extension: str) -> list:
    return [file for file in os.listdir(pathname) if file.endswith(file_extension)]


def delete_files(files: list):
    for f in files:
        os.remove(f)


def compile_config(src) -> dict:

    if type(src) is dict:
        print('Config successfully compiled')
        return src

    if type(src) is not str:
        raise TypeError('src config should be of type str or dict')

    with open(src, mode='r') as yam_file:
        config = yaml.load(yam_file, Loader=yaml.FullLoader)

    print('Config successfully compiled')
    return config


def load_module(module_uri: str, class_name: str, config: dict):
    module = importlib.import_module(module_uri)
    return module.__dict__[class_name](config)


def get_class_filename(class_object):
    return inspect.getfile(class_object.__class__).split('/')[-1].replace('.py', '')


class LoadPythonPackage:

    def __init__(self, request_modules: list, package_name: str):
        self.package_name = package_name
        self.request_modules = request_modules
        self.actual_modules = find_file_type(path(package_name), '.py')
        self.idx = 0
        self.idx_limit = len(request_modules)

    def __iter__(self):
        return self

    def __next__(self):

        if self.idx < self.idx_limit:

            if f'{self.request_modules[self.idx]}.py' not in self.actual_modules:
                out = None, self.request_modules[self.idx]
                self.idx += 1
                return out

            file_path = path(os.path.join(self.package_name, f'{self.request_modules[self.idx]}.py'))
            if os.path.getsize(path(file_path)) == 0:
                out = None, self.request_modules[self.idx]
                self.idx += 1
                return out

            module = importlib.import_module(f'{self.package_name}.{self.request_modules[self.idx]}')
            out = module, self.request_modules[self.idx]
            self.idx += 1
            return out

        else:
            raise StopIteration
