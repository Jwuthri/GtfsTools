
# GtfsTools [![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/)

> pip install is coming

For now you must clone this repo and add him to your PYTHON_PATH

### Installation

GtfsTools requires [Python 3.6](https://www.python.org/downloads/release/python-360/).

Install dependencies thanks to setup.py
```
$ python setup.py
```

### How it works ?

Work in progress....

### Run

```python
from mixer.gtfs.to_zip.controller import *

new_zip_path = Controller(gtfs_path=path_of_your_zip).main()
```
new_zip_path contains the path of the gtfs normalized, with shapes, stoptimes etc...

### Todos

 - Write Tests
 - Make pip install
 - ...

License
----

MIT


**Free Software !**
