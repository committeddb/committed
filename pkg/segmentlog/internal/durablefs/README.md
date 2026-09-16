# Durable file publication

This internal layer prepares filesystem primitives for the future segment-log
catalog. It has no knowledge of records, scrub generations, or recovery policy.
It currently supports local Linux/macOS filesystems with POSIX hard links, atomic
rename, and directory fsync. Other platforms fail at `Open` before creating files.
Network filesystem semantics and hardware durability are not established here.

The directory must already exist durably. The caller exclusively manages it,
serializes conflicting publications, and prevents directory replacement while
operations run. Names are single path components. Temporary files are mode 0600,
created in the same directory as their final destination.

## Operations

`Install(name, write)` installs an immutable file without replacing existing
content:

1. Create a unique temporary file and write through a checked writer.
2. Sync and close the file. Any write, short-write, sync, or close error prevents
   installation. A callback cannot hide a failed write by returning nil.
3. Hard-link the temporary file to the final name. A collision fails; it does not
   imply matching contents or permit overwriting.
4. Remove the temporary alias and sync the directory.

`Replace(name, write)` prepares the same synced temporary file, then atomically
renames it over the final name and syncs the directory. Its intended use is a
small pointer such as CURRENT. The owning log must have durably installed every
referenced file first. This primitive does not create a transaction across files.

Both return a `Result` and an error:

| Result | Meaning and caller obligation |
| --- | --- |
| No error; Installed and Durable true | File and final directory entry were synced. The owning log may advance its protocol. |
| Installed false; error | Final name was not changed by this call. Inspect cleanup errors and Temp; do not discard the original error. |
| Installed true; Durable false; ErrUncertain | Namespace publication succeeded but directory durability was not confirmed. Stop owning-log mutation and recover from disk. Do not unlink the final file as rollback. |
| Installed and Durable true; error and Temp set | Immutable file was installed durably, but its temporary hard-link alias remains. Report and clean up that alias under the owning log's policy. |

An installation can leave both an alias and uncertain durability; those fields
and errors are reported together. Uninstalled partial files are removed and the
removal directory is synced where possible. Cleanup errors are joined with the
original error, and Temp identifies a name whose removal failed. Successful
removal followed by a failed cleanup directory sync can still leave a crash
orphan, so recovery must inspect the directory even when Temp is empty.

The API reports observed syscall progress. The owning log must implement the
stop/recovery rule; this stateless helper does not poison future calls itself.
Callbacks must return normally and not retain/use their writer afterward. There
is no transaction or durability promise for callback side effects outside the
provided writer. Immutable files are immutable by ownership convention, not by
filesystem permissions.

## Crash boundaries and limits

Before installation, a crash may leave a partial temporary file. After link or
rename but before directory sync, namespace durability is uncertain. The owning
catalog recovery must select committed state, validate references, and reclaim
unreferenced files; it must never choose the newest filename as proof of commit.

Temporary aliases and orphan files can contain erased data. This layer exposes
cleanup failures but does not yet implement recovery or physical-retirement
completion. It must not be used to claim that an erasure has completed.

Tests inject failures at create, write, file sync, close, link/rename, unlink, and
directory sync; they check ordering, no-clobber behavior, partial cleanup, combined
failures, and uncertainty after publication. An integration test installs and
reads a real encoded segment. These are protocol and local-filesystem tests, not
power-loss proof. Filesystem crash testing remains an adoption requirement.
