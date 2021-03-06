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
source activate scape-py2 && nosetests tests/test_*.py
source activate scape-py3 && nosetests tests/test_*.py
```

To run tests with coverage:

```
source activate scape-py2
nosetests tests/test*.py --with-coverage --cover-package=scape --cover-erase --cover-html --cover-html-dir=cover-${CONDA_DEFAULT_ENV}

source activate scape-py3
nosetests tests/test*.py --with-coverage --cover-package=scape --cover-erase --cover-html --cover-html-dir=cover-${CONDA_DEFAULT_ENV}
```

