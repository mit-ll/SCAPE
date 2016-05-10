#!/bin/sh

USAGE="$0 <python version>"

if [ "$#" == "0" ]; then
	echo "$USAGE"
	exit 1
fi

PY=$1
ENV=scape-py${PY}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${DIR}

conda create --yes --name ${ENV} python=${PY} --file dependencies.txt 
source activate ${ENV}
pip install -r dependencies.pip.txt 
conda env export -f ${ENV}.yml

