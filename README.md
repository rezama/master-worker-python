# master-worker-python

Master-worker library for coordinating computation among cluster workers through a shared file system.

## Overview

This framework is developed primarily for coarse-grained distributed computing on a computer cluster.  It can be used in any situation where workers have access to a shared file system.

A master process creates input files in a `store` directory.  Each input file corresponds to a unit of work to be processed by workers.  For example, the input file could contain a number to be factorized.

A number of worker processes monitor the `store` directory for available work.  Once a worker identifies an available input file, it creates a lock file associated with the input file and attempts to lock it.  If the lock is successfully acquired, the worker proceeds to process the input and save the results in the corresponding output file.

Once all the output files are available, the server can collect them and proceed to create a new set of input files for the workers.

## Philosophy

Why communicate through the file system instead of the network?

It makes the process more transparent and easier to monitor and debug.

## Features

- Two-stage creation of input and output files to avoid collisions and corrupted files.
- Works on local and NFS-mounted file systems.
- Limit file system accesses to reduce the i/o load on a shared file system.
- Once a small fraction of the problems remain, the master process can delete older lock files to free up the problems for other workers.  This is useful when a worker may exit abnormally without freeing up the lock file.  It is also useful in clusters using slow and fast hardware alongside each other.
- Workers automatically exit after a certain amount of idle time, or after seeing a stop file created by the master.

## Requirements

python-gflags

## Usage

Replace the body of `master_create_input_files()` to create input files that are
appropriate for your application.

Replace the body of `worker_process_file_real()` with code to process your input
file and create the appropriate output.

## Usage on Condor

The included `condor_submit.txt` script launches a master process and three worker processes.  Launch the script using:

```bash
$ condor_submit condor_submit.txt
```

