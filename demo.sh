#!/bin/bash

./cleanup.sh

# Launch three workers in background.
python ./masterworker.py --role=worker --worker_id=1 &
python ./masterworker.py --role=worker --worker_id=2 &
python ./masterworker.py --role=worker --worker_id=3 &

# Launch the master process in foreground.
python ./masterworker.py --role=master

sleep 12
