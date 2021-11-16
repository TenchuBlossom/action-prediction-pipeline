import subprocess
import tools.file_system as fs

with open(fs.path('./conda_package.txt')) as file:
    lines = file.readlines()
    for cmd in lines:
        cmd_array = cmd.split()
        result = subprocess.run(cmd_array, timeout=60.0, check=True)
