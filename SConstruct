import os.path
from subprocess import *

# Get the number of cores on the system
cmd = 'grep -c processor /proc/cpuinfo'
p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
ncpu_opt = '-DJOS_NCPU=' + p.stdout.read().split('\n')[0]

# Let's define a common build environment first...
common_env = Environment()
common_env['CXX'] = 'gccfilter -c -a g++'
common_env.Append(CCFLAGS = ['-g', '-O3', '-Wall'],
        CPPFLAGS = [ncpu_opt,
        '-DJOS_CLINE=64', '-DCACHE_LINE_SIZE=64', '-D__STDC_FORMAT_MACROS'],
        CPPPATH = ['#.', '#lib/', '/usr/local/include/cbt'])

cbt_env = common_env.Clone()
cbt_env.Append(CPPFLAGS='-DAGG_DS=0')
cbt_env.VariantDir('obj/cbt', '.', duplicate=0)

htc_env = common_env.Clone()
htc_env.Append(CPPFLAGS='-DAGG_DS=1')
htc_env.VariantDir('obj/htc', '.', duplicate=0)

sh_env = common_env.Clone()
sh_env.Append(CPPFLAGS='-DAGG_DS=2')
sh_env.VariantDir('obj/sh', '.', duplicate=0)

nsort_env = common_env.Clone()
nsort_env.Append(CPPFLAGS='-DAGG_DS=3')
nsort_env.Append(LIBS='nsort')
nsort_env.VariantDir('obj/nsort', '.', duplicate=0)

# Now that all build environment have been defined, let's invoke the lower
# level SConscript files.
cbt_env.SConscript('obj/cbt/SConscript', {'env': cbt_env})
htc_env.SConscript('obj/htc/SConscript', {'env': htc_env})
sh_env.SConscript('obj/sh/SConscript', {'env': sh_env})
nsort_env.SConscript('obj/nsort/SConscript', {'env': nsort_env})
