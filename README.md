[![ci](https://github.com/flux-framework/flux-core/workflows/ci/badge.svg)](https://github.com/flux-framework/flux-core/actions?query=workflow%3A.github%2Fworkflows%2Fmain.yml)
[![codecov](https://codecov.io/gh/flux-framework/flux-core/branch/master/graph/badge.svg)](https://codecov.io/gh/flux-framework/flux-core)

_NOTE: The interfaces of flux-core are being actively developed
and are not yet stable._ The github issue tracker is the primary
way to communicate with the developers.

See also our [Online Documentation](https://flux-framework.readthedocs.io).

### flux-core

flux-core implements the communication layer and lowest level
services and interfaces for the Flux resource manager framework.
It consists of a distributed message broker, broker plug-in modules
that implement various distributed services, and an API and set
of utilities to utilize these services.

flux-core is intended to be the first building block used in the
construction of a site-composed Flux resource manager.  Other building
blocks are also in development under the
[flux-framework github organization](https://github.com/flux-framework),
including a fully functional workload
[scheduler](https://github.com/flux-framework/flux-sched).

Framework projects use the C4 development model pioneered in
the ZeroMQ project and forked as
[Flux RFC 1](https://flux-framework.rtfd.io/projects/flux-rfc/en/latest/spec_1.html).
Flux licensing and collaboration plans are described in
[Flux RFC 2](https://flux-framework.rtfd.io/projects/flux-rfc/en/latest/spec_2.html).
Protocols and API's used in Flux will be documented as Flux RFC's.

#### Build Requirements

<!-- A collapsible section with markdown -->
<details>
  <summary>Click to expand and see our full dependency table</summary>

flux-core requires the following packages to build:

**redhat**        | **ubuntu**        | **version**       | **note**
----------        | ----------        | -----------       | --------
autoconf          | autoconf          |                   |
automake          | automake          |                   |
libtool           | libtool           |                   |
make              | make              |                   |
pkgconfig         | pkg-config        |                   |
zeromq4-devel     | libzmq3-dev       | >= 4.0.4          |
czmq-devel        | libczmq-dev       | >= 3.0.1          |
jansson-devel     | libjansson-dev    | >= 2.6            |
libuuid-devel     | uuid-dev          |                   |
lz4-devel         | liblz4-dev        |                   |
ncurses-devel     | libncurses-dev    |                   |
hwloc-devel       | libhwloc-dev      | >= v1.11.1        |
sqlite-devel      | libsqlite3-dev    | >= 3.0.0          |
lua               | lua5.1            | >= 5.1, < 5.5     |
lua-devel         | liblua5.1-dev     | >= 5.1, < 5.5     |
lua-posix         | lua-posix         |                   |
python36-devel    | python3-dev       | >= 3.6            |
python36-cffi     | python3-cffi      | >= 1.1            |
python36-yaml     | python3-yaml      | >= 3.10.0         |
python36-jsonschema | python3-jsonschema | >= 2.3.0, < 4.0 |
phthon3-sphinx    | python3-sphinx    |                   | *1*

*Note 1 - only needed if optional man pages are to be created.

The following optional dependencies enable additional testing:

**redhat**        | **ubuntu**        | **version**
----------        | ----------        | -----------
aspell            | aspell            |
aspell-en         | aspell-en         |
valgrind-devel    | valgrind          |
mpich-devel       | libmpich-dev      |
jq                | jq                |
</details>

##### Installing RedHat/CentOS Packages
```
yum install autoconf automake libtool make pkgconfig zeromq4-devel czmq-devel libuuid-devel jansson-devel lz4-devel hwloc-devel sqlite-devel lua lua-devel lua-posix python36-devel python36-cffi python36-yaml python36-jsonschema python3-sphinx aspell aspell-en valgrind-devel mpich-devel jq
```

##### Installing Ubuntu Packages
```
apt install autoconf automake libtool make pkg-config libzmq3-dev libczmq-dev uuid-dev libjansson-dev liblz4-dev libhwloc-dev libsqlite3-dev lua5.1 liblua5.1-dev lua-posix python3-dev python3-cffi python3-yaml python3-jsonschema python3-sphinx aspell aspell-en valgrind libmpich-dev jq
```

##### Building from Source
```
./autogen.sh   # skip if building from a release tarball
./configure
make
make check
```

#### Bootstrapping a Flux instance

A Flux instance is composed of a set of `flux-broker` processes
that bootstrap via PMI (e.g. under another resource manager), or locally
via the `flux start` command.

No administrator privilege is required to start a Flux instance
as described below.

##### Single node session

To start a Flux instance (size = 8) on the local node for testing:
```
src/cmd/flux start --size 8
```
A shell is spawned that has its environment set up so that Flux
commands can find the message broker socket.  When the shell exits,
the session exits.

##### SLURM session

To start a Flux instance (size = 64) on a cluster using SLURM:
```
srun --pty --mpi=none -N64 src/cmd/flux start
```
The srun --pty option is used to connect to the rank 0 shell.
When you exit this shell, the session terminates.

#### Flux commands

Within a session, the path to the `flux` command associated with the
session broker will be prepended to `PATH`, so use of a relative or
absolute path is no longer necessary.

To see a list of commonly used commands run `flux` with no arguments,
`flux help`, or `flux --help`
```
$ flux help
Usage: flux [OPTIONS] COMMAND ARGS
  -h, --help             Display this message.
  -v, --verbose          Be verbose about environment and command search
  -V, --version          Display command and component versions
  -p, --parent           Set environment of parent instead of current instance

Common commands from flux-core:
   broker             Invoke Flux message broker daemon
   content            Access instance content storage
   cron               Schedule tasks on timers and events
   dmesg              manipulate broker log ring buffer
   env                Print the flux environment or execute a command inside it
   event              Send and receive Flux events
   exec               Execute processes across flux ranks
   get,set,lsattr     Access, modify, and list broker attributes
   hwloc              Control/query resource-hwloc service
   jobs               list jobs submitted to Flux
   keygen             generate keys for Flux security
   kvs                Flux key-value store utility
   logger             create a Flux log entry
   mini               Minimal Job Submission Tool
   job                Job Housekeeping Tool
   module             manage Flux extension modules
   ping               measure round-trip latency to Flux services
   proxy              Create proxy environment for Flux instance
   start              bootstrap a local Flux instance
   version            Display flux version information
```

Most of these have UNIX manual pages as `flux-<sub-command>(1)`,
which can also be accessed using `./flux help <sub-command>`.

#### A note about PMI

During launch, Flux brokers use a PMI server provided by the launcher
to exchange network endpoints and other information.  Flux also _provides_
a PMI server embedded in the Flux job shell to support launching parallel
applications such as MPI or subordinate Flux instances.  In both cases,
the PMI version 1 wire protocol is Flux's preferred PMI interface, but
other options are available.

See the [Flux FAQ](https://flux-framework.readthedocs.io/en/latest/faqs.html)
for more information on debugging and configuring Flux to interoperate with
foreign launchers and different MPI versions.  Open a flux-core issue if
you encounter a situation that is not covered in the FAQ.

#### Release

SPDX-License-Identifier: LGPL-3.0

LLNL-CODE-764420
