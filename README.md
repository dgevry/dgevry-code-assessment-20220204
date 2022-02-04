# code-assesment
## Problem Desciption
Assume you have 2 datasets

Dataset #1: This dataset stores the current information (assume this will have 300,000 rows)

Dataset #2: This dataset stores all history data in the append model. There is a column cob_date which tells you when the dataset was generated  (assume this will have 25 million rows)
Your goal: Create dataset which tells us the first time attribute_a, attribute_b, attribute_c were true
You will be evaluated on the quality of the code and testing. You can make any assumptions about the datatypes. Only acceptable language for this exercise is python and pyspark   

Dataset #1
```
Primary_key,Attribute_a,Attribute_B,Attribute_C,Current_date
1,Yes,No,No,01/02/2021
2,Yes,Yes,Yes,01/02/2021
3,No,No,No,01/02/2021
4,Yes,No,No,01/02/2021
```

Dataset #2
```
Primary_key,Attribute_a,Attribute_B,Attribute_C,Cob_date
1,Yes,No,No,01/01/2021
2,No,No,No,01/01/2021
3,Yes,Yes,Yes,01/01/2021
4,No,Yes,Yes,01/01/2021
1,Yes,No,No,01/02/2021
2,Yes,Yes,Yes,01/02/2021
3,No,No,No,01/02/2021
4,Yes,No,No,01/02/2021
```

Final Result
```
Primary_key,Attribute_a,Attribute_B,Attribute_C,Current_date,Days_since_attribute_a,Days_since_attribute_b,Days_since_attribute_c
1,Yes,No,No,01/02/2021,1,0,0
2,Yes,Yes,Yes,01/02/2021,0,0,0
3,No,No,No,01/02/2021,1,1,1
4,Yes,No,No,01/02/2021,0,1,1
```

## Assumptions and interpretations
For the above provide it was assumed that all data set would be communicated with csv inputs and output files.
It was assumed, based on data sample, that cases with no attribute value of Yes on the current or prior dates should be reported as a 0 for days since.
The key was assumed to be only an integer.
It was assumed that keys in the current dataset were the only keys that required reporting and keys included historically but not part of the current dataset will be ignored.

## Development Environment Setup

### Pre-requisites
Have workstation with
     - Docker installed and ruuning.
     - Ability pull docker base images

Note. We will be using Alpine Linux base images.
You can choose any other distribution, but will need address usage of package manager (yum,apt) accordingly.

### Build pyspark container

Navigate to root of this repository and run Docker build:
```bash
make build
```

### Setup

Launch very minimal conatainer in background and mount local folder with this repository content.

Variable setup Linux
```
WORK_FOLDER=`pwd`
CONTAINER_NAME="workstation"
DOCKER_IMAGE="my-pyspark:1.0.0"
````

Variable setup Windows(Powershell)
```
$WORK_FOLDER=(pwd).path
$CONTAINER_NAME="workstation"
$DOCKER_IMAGE="my-pyspark:1.0.0"
```

Docker run/exec commands for testing and cleanup
```
# Create
docker run -v ${WORK_FOLDER}:/work -d --name ${CONTAINER_NAME} ${DOCKER_IMAGE} tail -f /dev/null

# Exec
docker exec -it ${CONTAINER_NAME} /bin/bash

# Cleanup
docker stop ${CONTAINER_NAME}
docker rm ${CONTAINER_NAME}
```

## Code Assesment testing and usage

Inside development environment container to generate data and run test:
```bash
cd /work
make run
```

Script usage
```
python3 creator.py -h
```

Run on Sample Data
```
# Pass current and history datasets from csv files and produce the results to a csv file
# and show the resulting dataset truncated
    python3 ./creator.py -ds1 <dataset1.csv> -ds2 <dataset2.csv> -r <results.csv> --show Yes
For example:
cd /work
python3 ./creator.py -ds1 tests/simple-data/sample/input1.csv -ds2 tests/simple-data/sample/input2.csv -r results.csv --show Yes
```
## Project structure
```
.
├── Dockerfile
├── Makefile
├── README.md
├── creator.py
├── requirements.txt
├── test_data_generator.py
└── tests
    ├── __init__.py
    ├── dynamic-data
    │   └── max_history
    │       ├── expected.csv
    │       ├── input1.csv
    │       ├── input2.csv
    │       └── result.csv
    ├── simple-data
    │   ├── min_mid_max
    │   │   ├── expected.csv
    │   │   ├── input1.csv
    │   │   ├── input2.csv
    │   │   └── result.csv
    │   └── sample
    │       ├── expected.csv
    │       ├── input1.csv
    │       ├── input2.csv
    │       └── result.csv
    └── test_suite.py
```