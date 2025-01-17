.. flux-help-include: true

===========
flux-kvs(1)
===========


SYNOPSIS
========

**flux** **kvs** *COMMAND* [*OPTIONS*]


DESCRIPTION
===========

The Flux key-value store (KVS) is a simple, distributed data storage
service used a building block by other Flux components.
flux-kvs(1) is a command line utility that operates on the KVS.
It is a very thin layer on top of a C API.

The Flux KVS stores values under string keys. The keys are
hierarchical, using "." as a path separator, analogous to "/"
separated UNIX file paths. A single "." represents the root directory
of the KVS.

The KVS is distributed among the broker ranks of a Flux instance. Rank 0
is the leader, and other ranks are caching followers. All writes are flushed
to the leader during a commit operation. Data is stored in a hash tree
such that every commit results in a new root hash. Each new root hash
is multicast across the session. When followers update their root hash,
they atomically update their view to match the leader. There may be a
delay after a commit while old data is served on a follower that has not yet
updated its root hash, thus the Flux KVS consistency model is "eventually
consistent". Followers cache data temporally and fault in new data through
their parent in the overlay network.

Different KVS namespaces can be created in which kvs values can be
read from/written to. By default, all KVS operations operate on the
default KVS namespace "primary". An alternate namespace can be
specified in most kvs commands via the *--namespace* option, or by
setting the namespace in the environment variable FLUX_KVS_NAMESPACE.

flux-kvs(1) runs a KVS *COMMAND*. The possible commands and their
arguments are described below.


COMMANDS
========

**namespace create** [-o owner] [-r rootref] *name* [*name* ...]
   Create a new kvs namespace. User may specify an alternate userid of a
   user that owns the namespace via *-o*. Specifying an alternate owner
   would allow a non-instance owner to read/write to a namespace.
   User may specify an initial root reference for the namespace via
   *-r*.

**namespace remove** *name* [*name...*]
   Remove a kvs namespace.

**namespace list**
   List all current namespaces and info on each namespace.

**get** [-N ns] [-r|-t] [-a treeobj] [-l] [-W] [-w] [-u] [-A] [-f] [-c count] *key* [*key* ...]
   Retrieve the value stored under *key*. If nothing has been stored
   under *key*, display an error message. Specify an alternate namespace
   to retrieve *key* from via *-N*. If no options, value is displayed
   with a newline appended (if value length is nonzero). If *-l*, a
   *key=* prefix is added. If *-r*, value is displayed without a newline.
   If *-t*, the RFC 11 object is displayed. *-a treeobj* causes the
   lookup to be relative to an RFC 11 snapshot reference. If *-W* is
   specified and a key does not exist, wait until the key has been
   created. If *-w*, after the initial value, display the new value each
   time the key is written to until interrupted, or if *-c count* is
   specified, until *count* values have been displayed. If *-u* is
   specified, only writes that change the key value will be displayed.
   If *-A* is specified, only display appends that occur on a key. By
   default, only a direct write to a key is monitored, which may miss
   several unique situations, such as the replacement of an entire parent
   directory. The *-f* option can be specified to monitor for many of
   these special situations.

**put** [-N ns] [-O|-s] [-r|-t] [-n] [-A] *key=value* [*key=value* ...]
   Store *value* under *key* and commit it. Specify an alternate
   namespace to commit value(s) via *-N*. If it already has a value,
   overwrite it. If no options, value is stored directly. If *-r* or
   *-t*, the value may optionally be read from standard input if
   specified as "-". If *-r*, the value may include embedded NULL bytes.
   If *-t*, value is stored as a RFC 11 object. *-n* prevents the commit
   from being merged with with other contemporaneous commits. *-A*
   appends the value to a key instead of overwriting the value. Append
   is incompatible with the -j option. After a successful put, *-O* or
   *-s* can be specified to output the RFC11 treeobj or root sequence
   number of the root containing the put(s).

**ls** [-N ns] [-R] [-d] [-F] [-w COLS] [-1] [*key* ...]
   Display directory referred to by *key*, or "." (root) if unspecified.
   Specify an alternate namespace to display via *-N*. Remaining options are
   roughly equivalent to a subset of ls(1) options. *-R* lists directory
   recursively. *-d* displays directory not its contents. *-F*
   classifies files with one character suffix (. is directory, @ is
   symlink). *-w COLS* sets the terminal width in characters. *-1*
   causes output to be displayed in one column.

**dir** [-N ns] [-R] [-d] [-w COLS] [-a treeobj] [*key*]
   Display all keys and their values under the directory *key*. Specify
   an alternate namespace to display via *-N*. If *key* does not exist
   or is not a directory, display an error message. If *key* is not
   provided, "." (root of the namespace) is assumed. If *-R* is
   specified, recursively display keys under subdirectories. If *-d* is
   specified, do not output key values. Output is truncated to fit the
   terminal width. *-w COLS* sets the terminal width (0=unlimited). *-a
   treeobj* causes the lookup to be relative to an RFC 11 snapshot
   reference.

**unlink** [-N ns] [-O|-s] [-R] [-f] *key* [*key* ...]
   Remove *key* from the KVS and commit the change. Specify an alternate
   namespace to commit to via *-N*. If *key* represents a directory,
   specify *-R* to remove all keys underneath it. If *-f* is specified,
   ignore nonexistent files. After a successful unlink, *-O* or *-s* can
   be specified to output the RFC11 treeobj or root sequence number of
   the root containing the unlink(s).

**link** [-N ns] [-T ns] [-O|-s] *target* *linkname*
   Create a new name for *target*, similar to a symbolic link, and commit
   the change. *target* does not have to exist. If *linkname* exists,
   it is overwritten. Specify an alternate namespace to commit linkname
   to via *-N*. Specify the target's namespace via *-T*. After a
   successfully created link, *-O* or *-s* can be specified to output the
   RFC11 treeobj or root sequence number of the root containing the link.

**readlink** [-N ns] [-a treeobj] [ -o \| -k ] *key* [*key* ...]
   Retrieve the key a link refers to rather than its value, as would be
   returned by **get**. Specify an alternate namespace to retrieve from
   via *-N*. *-a treeobj* causes the lookup to be relative to an RFC 11
   snapshot reference. If the link points to a namespace, the namespace
   and key will be output in the format *<namespace>::<key>*. The *-o*
   can be used to only output namespaces and the *-k* can be used to only
   output keys.

**mkdir** [-N ns] [-O|-s] *key* [*key* ...]
   Create an empty directory and commit the change. If *key* exists,
   it is overwritten. Specify an alternate namespace to commit to via
   *-N*. After a successful mkdir, *-O* or *-s* can be specified to
   output the RFC11 treeobj or root sequence number of the root
   containing the new directory.

**copy** [-S src-ns] [-D dst-ns] *source* *destination*
   Copy *source* key to *destination* key. Optionally, specify a source
   and/or destination namespace for the *source* and/or *destination*
   respectively. If a directory is copied, a new reference is created;
   it is unnecessary for **copy** to recurse into *source*.

**move** [-S src-ns] [-D dst-ns] *source* *destination*
   Like **copy**, but *source* is unlinked after the copy.

**dropcache** [--all]
   Tell the local KVS to drop any cache it is holding. If *--all* is
   specified, send an event across the Flux instance instructing all KVS
   modules to drop their caches.

**version** [-N ns]
   Display the current KVS version, an integer value. The version starts
   at zero and is incremented on each KVS commit. Note that some commits
   may be aggregated for performance and the version will be incremented
   once for the aggregation, so it cannot be used as a direct count of
   commit requests. Specify an alternate namespace to retrieve the
   version from via *-N*.

**wait** [-N ns] *version*
   Block until the KVS version reaches *version* or greater. A simple form
   of synchronization between peers is: node A puts a value, commits it,
   reads version, sends version to node B. Node B waits for version, gets
   value.

**getroot** [-N ns] [-s \| -o \| -b]
   Retrieve the current KVS root, displaying it as an RFC 11 dirref object.
   Specify an alternate namespace to retrieve from via *-N*. If *-o* is
   specified, display the namespace owner. If *-s* is specified, display
   the root sequence number.  If *-b* is specified, display the root blobref.

**eventlog get** [-N ns] [-W] [-w] [-c count] [-u] *key*
   Display the contents of an RFC 18 KVS eventlog referred to by *key*.
   If *-u* is specified, display the log in raw form. If *-W* is
   specified and the eventlog does not exist, wait until it has been
   created. If *-w* is specified, after the existing contents have
   been displayed, the eventlog is monitored and updates are displayed
   as they are committed.  This runs until the program is interrupted
   or an error occurs, unless the number of events is limited with the
   *-c* option. Specify an alternate namespace to display from via
   *-N*.

**eventlog append** [-N ns] [-t SECONDS] *key* *name* [*context* ...]
   Append an event to an RFC 18 KVS eventlog referred to by *key*.
   The event *name* and optional *context* are specified on the command line.
   The timestamp may optionally be specified with *-t* as decimal seconds since
   the UNIX epoch (UTC), otherwise the current wall clock is used.
   Specify an alternate namespace to append to via *-N*.

**eventlog wait-event** [-N ns] [-t SECONDS] [-u] [-W] [-q] [-v] *key* *event*
   Wait for a specific *event* to occur in an RFC 18 KVS eventlog
   referred to by *key*.  If *-t* is specified, timeout after
   *SECONDS* if the event has not occurred.  If *-u* is specified,
   display the log in raw form. If *-W* is specified and the eventlog
   does not exist, wait until it has been created. If *-q* is
   specified, not output the matched event.  If *-v* is specified,
   output all events prior to the matched event.  This runs until the
   program is interrupted, the event occurs, or a timeout occurs if
   *-t* is specified.  Specify an alternate namespace to display from
   via *-N*.


RESOURCES
=========

Github: http://github.com/flux-framework
