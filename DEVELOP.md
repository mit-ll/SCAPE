# Conda environment

Create python 2 and 3 development environments:

```
conda env create -f conda/scape-py2.yml
conda env create -f conda/scape-py3.yml
```

These files are created with:

```
./conda/create_envs.sh <python version>
```

# Testing

To run unit tests:

```
nosetests tests/registry_test.py
```

To run tests with coverage:

```
source activate scape-py2
nosetests tests/registry_test.py --with-coverage --cover-erase --cover-html --cover-html-dir=cover-py2

source activate scape-py3
nosetests tests/registry_test.py --with-coverage --cover-erase --cover-html --cover-html-dir=cover-py3
```

