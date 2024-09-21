# algorithm-poc
This repository has example code for bloom filter, cuckoo filter, count min, hyper log log and topk using redis stack.
# Installation
- Install docker (to run redis stack)
- Install python (3.12.3 +)
- Run redis stack `docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest`
- Install python dependies `pip install -r requirements.txt`

## Create a virtual python environment
If "error: externally-managed-environment This environment is externally managed" message is observed on pip install commands
Run the following commands:
To activate virtual environment
- `python -m venv venv-poc`
- `source ./venv-poc/bin/activate`
To deactivate virtual environment
- `deactivate`

# Files
- Bloom Filter `python ds/rbloom.py`
- Count-min `python ds/rcountmin.py`
- Cuckoo Filter `python ds/rcuckoo.py`
- Hyper Log Log `python ds/rhyperloglog.py`
- Top-K `python ds/rtopk.py`
- Load data to redis `python utils/load_data.py`
- Main app `python app.py`

# About Anomaly
The provided data set has the following column data:
- `ip`: ip address of click.
- `app`: app id for marketing.
- `device`: device type id of user mobile phone (e.g., iphone 6 plus, iphone 7, huawei mate 7, etc.)
- `os`: os version id of user mobile phone
- `channel`: channel id of mobile ad publisher
- `click_time`: timestamp of click (UTC)
- `attributed_time`: if user download the app for after clicking an ad, this is the time of the app download
- `is_attributed`: the target that is to be predicted, indicating the app was downloaded

## Anomaly Conditions
- `ip`-`device`: An `ip` alone can be unique but there are cases where multiple devices have same `ip` therefore a combination of `ip` and `device` is better suited.
- An `app` can have multiple `channel`. As observed for `ip` 73487 has `channel` 178, 265 against `app` 12. Therefore, `ip`-`device`-`channel` can be a better key.
- We can assume that `ip`-`device` will have a fixed `os`. For example, a device such as a laptop can have a unique operating system. So changes to `os` for a `ip`-`device` combination can be considered fraud.
