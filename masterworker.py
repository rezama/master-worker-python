#!/usr/bin/env python
"""Master/worker framework."""

import fcntl
import logging
import os
import random
import shutil
import socket
import sys
import time

import gflags

# Constants used in flag definitions.
ROLE_MASTER = 'master'
ROLE_WORKER = 'worker'

FLAGS = gflags.FLAGS
# Common flags.
gflags.DEFINE_enum('role', None, [ROLE_MASTER, ROLE_WORKER],
                   'Specify which role to execute.')
gflags.MarkFlagAsRequired('role')

# Master flags.
gflags.DEFINE_integer('process_secs', 10,
                      'Estimated time needed for processing each problem.')
gflags.DEFINE_float('lockfile_timeout_threshold', .05,
                    'Master starts deleting old lock files once only this '
                    'fraction of the population is left for processing.')
gflags.DEFINE_bool('master_does_evaluation', False,
                   'Whether the master evaluates genomes as well (no need for '
                   'workers).')
gflags.DEFINE_integer('master_idle_sleep_secs', 20,
                      'How long to sleep when output files from workers are '
                      'not ready.')

# Worker flags.
gflags.DEFINE_bool('take_lockfile_existence_as_lock', True,
                   'Whether a worker should respect an existing lock file from '
                   'other workers.')
gflags.DEFINE_string('tmp_folder', '/tmp/masterworker',
                     'Path to tmp folder to use.  Used only when use_tmp=true.')
gflags.DEFINE_bool('use_tmp', False,
                   'Whether to store temporary files on a tmp device.')
gflags.DEFINE_integer('worker_id', None,
                      'ID for worker instance.  Starts from 1.')
gflags.DEFINE_integer('worker_idle_sleep_secs', 5,
                      'How long to sleep when a worker cannot find work.')
gflags.DEFINE_integer('worker_max_idle_secs', 10,
                      'Maximum idle time for a worker before worker will quit. '
                      'Note that on condor the master process might '
                      'occasionally get suspended for a while.')

# Constants.
FOLDER_STORE = 'store'
FILENAME_PROBLEM = 'problem'
FILENAME_INPUTTEMP = 'intemp'
FILENAME_INPUT = 'input'
FILENAME_OUTPUTTEMP = 'outtemp'
FILENAME_OUTPUT = 'output'
FILENAME_LOCK = '.lock'

STOP_FILE = 'stop'

SLEEP_SECONDS_PRETEND_WORKING = 3
N_PROBLEMS = 10

# Global logger.
LOG_FORMATTER = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
CONSOLE_HANDLER = logging.StreamHandler(sys.stdout)
CONSOLE_HANDLER.setFormatter(LOG_FORMATTER)
LOG = logging.getLogger()
LOG.setLevel(logging.DEBUG)
LOG.addHandler(CONSOLE_HANDLER)

def parse_flags(argv):
  """Parse flags."""
  try:
    argv = FLAGS(argv)  # Parse flags.
  except gflags.FlagsError, exc:
    print_usage()
    print
    print '%s\n' % exc
    sys.exit(1)

def print_usage():
  """Prints usage."""
  print 'Usage: %s --role=master' % sys.argv[0]
  print 'Usage: %s --role=worker --worker_id=<id>' % sys.argv[0]

def try_get_lock(path_to_lock_file):
  """Try to get lock. Return its file descriptor or -1 if failed.

  Args:
    path_to_lock_file: Name of file used as lock (i.e. '/var/lock/myLock').

  Returns:
    File descriptor of lock file, or -1 if failed.
  """
  mask = os.umask(0)
  lock_fd = os.open(path_to_lock_file, os.O_RDWR | os.O_CREAT, 0666)
  os.umask(mask)
  if lock_fd >= 0:
    try:
      fcntl.lockf(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
      os.close(lock_fd)
      lock_fd = -1
  return lock_fd

def release_lock(lock_fd, path_to_lock_file):
  """Release the lock obtained with try_get_lock(path_to_lock_file).

  Args:
    lock_fd: File descriptor of lock returned by try_get_lock().
    path_to_lock_file: Name of file used as lock (i.e. '/var/lock/myLock').

  Returns:
    None.
  """
  if lock_fd < 0:
    return
  try:
    os.remove(path_to_lock_file)
  except OSError:
    pass
  os.close(lock_fd)

def worker(worker_id):
  """Launches a worker, scanning store and processing files."""
  LOG.info('Launching worker ' + str(worker_id) + '...')
  time_last_working = time.time()
  time_now = time.time()
  time_idle_secs = time_now - time_last_working
  stop_file_seen = False

  try:
    if FLAGS.use_tmp:
      os.mkdir(FLAGS.tmp_folder)
    else:
      shutil.rmtree(FLAGS.tmp_folder)
  except OSError:
    pass

  while time_idle_secs < FLAGS.worker_max_idle_secs:
    LOG.info('Scanning store...')
    # store_files[] will end up having entries like:
    # problem-0-input
    # problem-1-input
    # ...
    # problem-9-input
    store_files = []
    try:
      store_files = os.listdir(FOLDER_STORE)
    except OSError as exc:
      LOG.error('Caught exception OSError: %s', exc)
      LOG.error('Continuing execution...')

    if STOP_FILE in store_files:
      stop_file_seen = True
      LOG.info('Stop file seen.  Exiting.')
      break
    LOG.info('Compiled file list.')
    LOG.info('Processing...')
    # Shuffle the store_files to reduce worker collisions.
    random.shuffle(store_files)
    # Prepend store path to create full file paths.
    all_paths_in_store = [os.path.join(FOLDER_STORE, filename)
                          for filename in store_files]
    for path_to_file in all_paths_in_store:
      did_work = worker_inspect_dir_entry(worker_id, path_to_file,
                                          all_paths_in_store)
      if did_work:
        time_last_working = time.time()

    time_now = time.time()
    time_idle_secs = time_now - time_last_working
    LOG.info('Idle for %ds.', time_idle_secs)
    LOG.info('Sleeping for %ds...', FLAGS.worker_idle_sleep_secs)
    time.sleep(FLAGS.worker_idle_sleep_secs)
  if not stop_file_seen:
    LOG.critical('Starvation. Worker cannot find work. Exiting.')

def worker_inspect_dir_entry(worker_id, path_to_file, all_paths_in_store):
  """Inspects a file in the store directory and processes it if it matches the
  pattern for an input file.
  """
  did_work = False
  if ((FILENAME_INPUT in path_to_file) and
      (FILENAME_LOCK not in path_to_file)):
    path_to_lock_file = path_to_file + FILENAME_LOCK
    path_to_output_file = path_to_file
    path_to_output_file = path_to_output_file.replace(FILENAME_INPUT,
                                                      FILENAME_OUTPUT)
    should_consider_file = False
    if FLAGS.take_lockfile_existence_as_lock:
      # Reduce io load:
      # Check the all_paths_in_store array before accessing disk.
      if ((path_to_output_file not in all_paths_in_store) and
          (path_to_lock_file not in all_paths_in_store)):
        should_consider_file = (os.path.isfile(path_to_file) and
                                not os.path.isfile(path_to_output_file) and
                                not os.path.isfile(path_to_lock_file))
    else:
      # Reduce io load.:
      # Check the all_paths_in_store array before accessing disk.
      if path_to_output_file not in all_paths_in_store:
        should_consider_file = (os.path.isfile(path_to_file) and
                                not os.path.isfile(path_to_output_file))
    if should_consider_file:
      lock_fd = try_get_lock(path_to_lock_file)
      if lock_fd > 0:
        did_work = worker_process_file(worker_id, path_to_file)
        release_lock(lock_fd, path_to_lock_file)
  return did_work

def worker_process_file(worker_id, path_to_file):
  """Processes an entry in store directory."""
  did_work = False
  time_start_processing = time.time()
  LOG.info('--')
  LOG.info('Processing file ' + path_to_file)
  path_to_output_file = path_to_file
  path_to_output_file = path_to_output_file.replace(FILENAME_INPUT,
                                                    FILENAME_OUTPUT)
  LOG.info('Output file is: ' + path_to_output_file)
  path_to_temp_output_file = path_to_file
  path_to_temp_output_file = (
      path_to_temp_output_file.replace(FILENAME_INPUT, FILENAME_OUTPUTTEMP))
  # Reduce io load.
  if FLAGS.use_tmp:
    path_to_temp_output_file = (FLAGS.tmp_folder + '/' +
                                os.path.basename(path_to_temp_output_file))
  hostname = socket.gethostname()
  filename_suffix = '.worker' + str(worker_id) + '.' + hostname
  path_to_temp_output_file += filename_suffix
  LOG.info('Temp output file is: ' + path_to_temp_output_file)

  if os.path.isfile(path_to_file):
    did_work = worker_process_file_real(path_to_file,
                                        path_to_temp_output_file, worker_id,
                                        hostname)
    if os.path.isfile(path_to_temp_output_file):
      LOG.info('Temp output file is ready.')
      if os.path.isfile(path_to_file):
        if not os.path.isfile(path_to_output_file):
          LOG.info('Renaming to output file.')
          # Reduce io load.
          if FLAGS.use_tmp:
            shutil.move(path_to_temp_output_file, path_to_output_file)
          else:
            os.rename(path_to_temp_output_file, path_to_output_file)
        else:
          LOG.info('Output file ' + path_to_file +
                   ' is already present. Discarding.')
          os.remove(path_to_temp_output_file)
          did_work = False
      else:
        LOG.info('Input file ' + path_to_file +
                 ' is already deleted.  Discarding.')
        os.remove(path_to_temp_output_file)
        did_work = False
    else:
      LOG.info('Temp output file was not generated.')
  else:
    LOG.info('Input file is already deleted. Aborting.')

  LOG.info('Done processing file ' + path_to_file)
  time_finish_processing = time.time()
  time_processing_secs = time_finish_processing - time_start_processing
  LOG.info('Worker ' + str(worker_id) + ' on ' + hostname + ' spent ' +
           str(time_processing_secs) + 's on last problem.')
  return did_work

def worker_process_file_real(path_to_input_file, path_to_temp_output_file,
                             worker_id, hostname):
  """Application-specific logic for reading the input file, generating the
  answer, and saving it in the output file.
  """
  time.sleep(SLEEP_SECONDS_PRETEND_WORKING)
  with open(path_to_temp_output_file, 'w') as output_file:
    output_file.write('Solution to problem in file %s.\n' % path_to_input_file)
    output_file.write('Done by worker %s on host %s.\n' % (worker_id, hostname))
  did_work = True
  return did_work

def master():
  """Launches a master.  Creates a series of problem files and waits for
  workers to process them and produce the answers.
  """
  LOG.info('Launching master...')
  LOG.info('Creating input files...')
  master_create_input_files()
  LOG.info('Input files created.')
  master_wait_for_output_files()
  LOG.info('All output files are ready.')
  # This demo creates a single set of input files.  Once they are processed,
  # the stop signal is sent to the workers.
  path_to_stop_file = os.path.join(FOLDER_STORE, STOP_FILE)
  open(path_to_stop_file, 'w').close()
  LOG.info('Done.')

def master_remove_old_locks():
  """Removed lock files that are older than the processing timeout."""
  for i in range(N_PROBLEMS):
    path_to_lock_file = os.path.join(FOLDER_STORE,
                                     FILENAME_PROBLEM + '-' + str(i) +
                                     '-' + FILENAME_INPUT + FILENAME_LOCK)
    if os.path.isfile(path_to_lock_file):
      try:
        time_file_modified = os.path.getmtime(path_to_lock_file)
      except OSError:
        time_file_modified = time.time()
      time_now = time.time()
      time_age_secs = time_now - time_file_modified
      if time_age_secs > FLAGS.process_secs:
        LOG.info('Lock file %s is %.1fs old.  Deleting lock file...',
                 path_to_lock_file, time_age_secs)
        try:
          os.remove(path_to_lock_file)
        except OSError:
          pass
      else:
        LOG.info('NOT DELETING: Lock file %s is %.1fs old.',
                 path_to_lock_file, time_age_secs)

def master_create_input_files():
  """Create problem files."""
  try:
    os.mkdir(FOLDER_STORE)
  except OSError:
    pass

  for i in range(N_PROBLEMS):
    # Reduce io load..
    path_to_temp_input_file = os.path.join(FOLDER_STORE,
                                           FILENAME_PROBLEM + '-' + str(i) +
                                           '-' + FILENAME_INPUTTEMP)
    path_to_input_file = os.path.join(FOLDER_STORE,
                                      FILENAME_PROBLEM + '-' + str(i) +
                                      '-' + FILENAME_INPUT)

    # LOG.info('Creating input file ' + path_to_temp_input_file)
    with open(path_to_temp_input_file, 'w') as input_file:
      input_file.write('Problem %d.\n' % i)

    # Reduce io load..
    if FLAGS.use_tmp:
      shutil.move(path_to_temp_input_file, path_to_input_file)
    else:
      os.rename(path_to_temp_input_file, path_to_input_file)

def master_wait_for_output_files():
  """Loop until workers have processed all input files."""
  is_all_processed = False
  while not is_all_processed:
    LOG.info('Sleeping for ' + str(FLAGS.master_idle_sleep_secs) + 's')
    time.sleep(FLAGS.master_idle_sleep_secs)
    LOG.info('Scanning output files...')
    is_all_processed = True
    num_outputs_remaining = 0
    for i in range(N_PROBLEMS):
      path_to_output_file = os.path.join(FOLDER_STORE,
                                         FILENAME_PROBLEM + '-' + str(i) +
                                         '-' + FILENAME_OUTPUT)
      if not os.path.isfile(path_to_output_file):
        is_all_processed = False
        num_outputs_remaining += 1
    LOG.info('Waiting on %d output files.', num_outputs_remaining)
    if not is_all_processed:
      percentage_remaining = float(num_outputs_remaining) / N_PROBLEMS
      if percentage_remaining < FLAGS.lockfile_timeout_threshold:
        master_remove_old_locks()

def main(argv):
  """Entry point of program."""
  parse_flags(argv)

  if FLAGS.role == ROLE_MASTER:
    master()
  elif FLAGS.role == ROLE_WORKER:
    assert FLAGS.worker_id > 0
    worker(FLAGS.worker_id)
  else:
    print 'Unrecognized role.\n'
    print_usage()
    sys.exit(1)

if __name__ == '__main__':
  main(sys.argv)
