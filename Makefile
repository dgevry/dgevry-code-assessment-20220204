SHELL := /bin/bash

image_version=1.0.0
image_name=my-pyspark

help:	## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

build:	## Create "workstation" Docker Image. This target should work work on Linux and Windows
	docker build -t ${image_name}:${image_version} .

setup:	## Installs python dependencies
	export PYTHONPATH=.
	python3 -m pip install -r requirements.txt --user

lint:	##Runs pylint on all python files
lint: ; @for py in *.py; do echo "Linting $$py"; python3 -m pylint -rn $$py; done

test:	## Runs the unit testsd for the code assessment using the sample and a modified simple case as well as full volume set.
	python3 -m unittest tests/test_suite.py -v

generate-data:	## Generates the simple and volume test data for the code-assessment testing
	python3 test_data_generator.py

run: ## This generates data and then runs the test suite
run: generate-data test 
