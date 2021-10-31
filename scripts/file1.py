import use_context
from tools.performance_profile_tools import PerformanceProfile
from file2 import function

config = {'name': 'sync', 'overwrite_profile': True}
use_context.performance_profile = PerformanceProfile(config)
function()

