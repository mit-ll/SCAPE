# Running pyspark with Anaconda IPython and python

(http://stackoverflow.com/a/32094874/1869370)[source]

```bash
export ANACONDA3_PATH=${HOME}/anaconda3

alias spark_ipython='PYSPARK_PYTHON=${ANACONDA3_PATH}/bin/python PYSPARK_DRIVER_PYTHON=ipython pyspark'
alias spark_notebook='PYSPARK_DRIVER_PYTHON_OPTS="notebook" spark_ipython'
```
