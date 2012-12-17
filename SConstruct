### SCons build recipe for Anon

# Important targets:
#
# build     - build the software (default)
# install   - install library
#
# audit     - run code-auditing tools
#
# TODO:
# uninstall - undo an install
# check     - run regression and unit tests.


import os.path
from subprocess import *

env = Environment()

src_files = [Glob('lib/*.cc')]

cmd = 'grep -c processor /proc/cpuinfo'
p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
ncpu_opt = '-DJOS_NCPU=' + p.stdout.read().split('\n')[0]

env.Append(CCFLAGS = ['-g', '-O3', '-Wall'],
            CPPFLAGS = [ncpu_opt,
            '-DJOS_CLINE=64', '-DCACHE_LINE_SIZE=64', '-D__STDC_FORMAT_MACROS',
            '-I.', '-Ilib/', '-I/usr/local/include/cbt'])
anonlib = env.StaticLibrary('obj/anon', src_files,
            LIBS = ['-ljemalloc', '-lnuma', '-lc', '-lm', '-lcbt', '-lpthread'],
            LINKFLAGS = ['--static'])

wc_files = ['app/wc.cc', 'app/wc_proto.cpp', 'app/wcproto.pb.cc']
wc_app = env.Program('obj/wc', wc_files,
            LIBPATH = ['-Lobj/'],
            LIBS = ['-lanon', '-lprotobuf', '-lpthread', '-lcbt', '-lsnappy',
                    '-lprofiler'])

## Targets
# build targets
build = env.Alias('build', [anonlib])

apps = env.Alias('apps', [wc_app])

env.Default(*build)
