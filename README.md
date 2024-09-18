# algorithm-poc
This repository has example code for bloom filter, cuckoo filter, count min, hyper log log and topk using redis stack.
# Installation
- Install docker (to run redis stack)
- Install python (3.12.3 +)
- Run redis stack `docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest`
- Install python dependies `pip install -r requirements.txt`

## Create a virtual python environment
If "error: externally-managed-environment This environment is externally managed" message is observed on pip install commands
Run the following commands
- `python -m venv venv-poc`
- `source ./venv/bin/activate`

# Run
- Bloom Filter `python rbloom.py`
- Count-min `python rcountmin.py`
- Cuckoo Filter `python rcuckoo.py`
- Hyper Log Log `python rhyperloglog.py`
- Top-K `python rtopk.py`

