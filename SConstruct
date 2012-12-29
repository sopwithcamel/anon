import os.path
from subprocess import *

# Get the number of cores on the system
cmd = 'grep -c processor /proc/cpuinfo'
p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
ncpu_opt = '-DJOS_NCPU=' + p.stdout.read().split('\n')[0]

# Let's define a common build environment first...
common_env = Environment()
common_env.Append(CCFLAGS = ['-g', '-O3', '-Wall'],
        CPPFLAGS = [ncpu_opt,
        '-DJOS_CLINE=64', '-DCACHE_LINE_SIZE=64', '-D__STDC_FORMAT_MACROS',
        '-I.', '-Ilib/', '-I/usr/local/include/cbt'])
cbt_env = common_env.Clone()
cbt_env.Append(CPPFLAGS='-DAGG_DS=0')
cbt_env.VariantDir('obj/cbt', '.', duplicate=0)

htc_env = common_env.Clone()
htc_env.Append(CPPFLAGS='-DAGG_DS=1')
htc_env.VariantDir('obj/htc', '.', duplicate=0)

# Now that all build environment have been defined, let's iterate over
# them and invoke the lower level SConscript files.
cbt_env.SConscript('obj/cbt/SConscript', {'env': cbt_env})
htc_env.SConscript('obj/htc/SConscript', {'env': htc_env})
