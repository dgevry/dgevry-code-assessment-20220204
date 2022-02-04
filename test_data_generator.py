#!/usr/bin/env python3
"""
This is utility to generate test data sets
"""

import csv
from datetime import datetime,timedelta
import os

MAX_CURRENT=300000
MAX_HISTORY=25000000


CURRENT_HEADER=[
    "Primary_key","Attribute_a","Attribute_B","Attribute_C","Current_date"
]
HISTORY_HEADER=[
    "Primary_key","Attribute_a","Attribute_B","Attribute_C","Cob_date"
]
RESULT_HEADER=[
    "Primary_key","Attribute_a","Attribute_B","Attribute_C","Current_date",
    "Days_since_attribute_a","Days_since_attribute_b","Days_since_attribute_c"
]

TEST_BASE_FOLDER =  os.path.join(os.getcwd(),"tests")
TEST_DATA_IN1 = "input1.csv"
TEST_DATA_IN2 = "input2.csv"
TEST_DATA_OUT = "expected.csv"

def generate_folder_structure():
    """
    Create test data folders
    """
    if not os.path.exists(TEST_BASE_FOLDER):
        os.mkdir(TEST_BASE_FOLDER)
    if not os.path.exists(os.path.join(TEST_BASE_FOLDER,"simple-data")):
        os.mkdir(os.path.join(TEST_BASE_FOLDER,"simple-data"))
    if not os.path.exists(os.path.join(TEST_BASE_FOLDER,"dynamic-data")):
        os.mkdir(os.path.join(TEST_BASE_FOLDER,"dynamic-data"))

def dict_to_csv(csv_file,dict_data,csv_columns):
    """
    Create csv file from dictionary
    """
    try:
        with open(csv_file, "w",encoding="utf8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in dict_data:
                writer.writerow(data)
    except IOError:
        print(f"Failed generate file: {csv_file}")

def generate_sample():
    """
    Generate Data sets as described in coding exam definition
    """
    test_name="sample"
    data1=[
        {"Primary_key": 1,
            "Attribute_a": "Yes","Attribute_B": "No","Attribute_C": "No",
            "Current_date": "01/02/2021"},
        {"Primary_key": 2,
            "Attribute_a": "Yes","Attribute_B": "Yes","Attribute_C": "Yes",
            "Current_date": "01/02/2021"},
        {"Primary_key": 3,
            "Attribute_a": "No","Attribute_B": "No","Attribute_C": "No",
            "Current_date": "01/02/2021"},
        {"Primary_key": 4,
            "Attribute_a": "Yes","Attribute_B": "No","Attribute_C": "No",
            "Current_date": "01/02/2021"},
    ]
    data2=[
        {"Primary_key": 1,
            "Attribute_a": "Yes","Attribute_B": "No","Attribute_C": "No",
            "Cob_date": "01/01/2021"},
        {"Primary_key": 2,
            "Attribute_a": "No","Attribute_B": "No","Attribute_C": "No",
            "Cob_date": "01/01/2021"},
        {"Primary_key": 3,
            "Attribute_a": "Yes","Attribute_B": "Yes","Attribute_C": "Yes",
            "Cob_date": "01/01/2021"},
        {"Primary_key": 4,
            "Attribute_a": "No","Attribute_B": "Yes","Attribute_C": "Yes",
            "Cob_date": "01/01/2021"},
        {"Primary_key": 1,
            "Attribute_a": "Yes","Attribute_B": "No","Attribute_C": "No",
            "Cob_date": "01/02/2021"},
        {"Primary_key": 2,
            "Attribute_a": "Yes","Attribute_B": "Yes","Attribute_C": "Yes",
            "Cob_date": "01/02/2021"},
        {"Primary_key": 3,
            "Attribute_a": "No","Attribute_B": "No","Attribute_C": "No",
            "Cob_date": "01/02/2021"},
        {"Primary_key": 4,
            "Attribute_a": "Yes","Attribute_B": "No","Attribute_C": "No",
            "Cob_date": "01/02/2021"},
    ]
    data3=[
        {"Primary_key": 1,
            "Attribute_a": "Yes","Attribute_B": "No","Attribute_C": "No",
            "Current_date": "01/02/2021",
            "Days_since_attribute_a": 1,"Days_since_attribute_b": 0,"Days_since_attribute_c": 0},
        {"Primary_key": 2,
            "Attribute_a": "Yes","Attribute_B": "Yes","Attribute_C": "Yes",
            "Current_date": "01/02/2021",
            "Days_since_attribute_a": 0,"Days_since_attribute_b": 0,"Days_since_attribute_c": 0},
        {"Primary_key": 3,
            "Attribute_a": "No","Attribute_B": "No","Attribute_C": "No",
            "Current_date": "01/02/2021",
            "Days_since_attribute_a": 1,"Days_since_attribute_b": 1,"Days_since_attribute_c": 1},
        {"Primary_key": 4,
            "Attribute_a": "Yes","Attribute_B": "No","Attribute_C": "No",
            "Current_date": "01/02/2021",
            "Days_since_attribute_a": 0,"Days_since_attribute_b": 1,"Days_since_attribute_c": 1},
    ]

    test_folder=os.path.join(TEST_BASE_FOLDER,"simple-data",test_name)
    if not os.path.exists(test_folder):
        os.mkdir(test_folder)

    dict_to_csv(os.path.join(test_folder,TEST_DATA_IN1),data1,CURRENT_HEADER)
    dict_to_csv(os.path.join(test_folder,TEST_DATA_IN2),data2,HISTORY_HEADER)
    dict_to_csv(os.path.join(test_folder,TEST_DATA_OUT),data3,RESULT_HEADER)

def generate_min_mid_max():
    """
    Generate Data sets where:
    key1
     - a: Yes on MIN(date) - only first date in historical data range
     - b: Yes on MID(date) - mid historical data range
     - c: Yes on MAX(date) - current
    key2
     - a: Never Yes
     - b: Always Yes
     - c: Never Yes
    """
    test_name="min_mid_max"
    data1=[
        {"Primary_key": 1,
            "Attribute_a": "No","Attribute_B": "No","Attribute_C": "Yes",
            "Current_date": "01/05/2021"},
        {"Primary_key": 2,
            "Attribute_a": "No","Attribute_B": "Yes","Attribute_C": "No",
            "Current_date": "01/05/2021"}
    ]
    data2=[
        {"Primary_key": 1,
            "Attribute_a": "Yes","Attribute_B": "No","Attribute_C": "No",
            "Cob_date": "01/01/2021"},
        {"Primary_key": 2,
            "Attribute_a": "No","Attribute_B": "Yes","Attribute_C": "No",
            "Cob_date": "01/01/2021"},
        {"Primary_key": 1,
            "Attribute_a": "No","Attribute_B": "No","Attribute_C": "No",
            "Cob_date": "01/02/2021"},
        {"Primary_key": 2,
            "Attribute_a": "No","Attribute_B": "Yes","Attribute_C": "No",
            "Cob_date": "01/02/2021"},
        {"Primary_key": 1,
            "Attribute_a": "No","Attribute_B": "Yes","Attribute_C": "No",
            "Cob_date": "01/03/2021"},
        {"Primary_key": 2,
            "Attribute_a": "No","Attribute_B": "Yes","Attribute_C": "No",
            "Cob_date": "01/03/2021"},
        {"Primary_key": 1,
            "Attribute_a": "No","Attribute_B": "No","Attribute_C": "No",
            "Cob_date": "01/04/2021"},
        {"Primary_key": 2,
            "Attribute_a": "No","Attribute_B": "Yes","Attribute_C": "No",
            "Cob_date": "01/04/2021"},
        {"Primary_key": 1,
            "Attribute_a": "No","Attribute_B": "No","Attribute_C": "Yes",
            "Cob_date": "01/05/2021"},
        {"Primary_key": 2,
            "Attribute_a": "No","Attribute_B": "Yes","Attribute_C": "No",
            "Cob_date": "01/05/2021"},
    ]
    data3=[
        {"Primary_key": 1,
            "Attribute_a": "No","Attribute_B": "No","Attribute_C": "Yes",
            "Current_date": "01/05/2021",
            "Days_since_attribute_a": 4,"Days_since_attribute_b": 2,"Days_since_attribute_c": 0},
        {"Primary_key": 2,
            "Attribute_a": "No","Attribute_B": "Yes","Attribute_C": "No",
            "Current_date": "01/05/2021",
            "Days_since_attribute_a": 0,"Days_since_attribute_b": 4,"Days_since_attribute_c": 0},
    ]

    test_folder=os.path.join(TEST_BASE_FOLDER,"simple-data",test_name)
    if not os.path.exists(test_folder):
        os.mkdir(test_folder)

    dict_to_csv(os.path.join(test_folder,TEST_DATA_IN1),data1,CURRENT_HEADER)
    dict_to_csv(os.path.join(test_folder,TEST_DATA_IN2),data2,HISTORY_HEADER)
    dict_to_csv(os.path.join(test_folder,TEST_DATA_OUT),data3,RESULT_HEADER)


def generate_current(test_name):
    """
    Generate current input file and expected results for Volume Test
    """
    today = datetime.today().strftime('%m/%d/%Y')
    test_folder=os.path.join(TEST_BASE_FOLDER,"dynamic-data",test_name)
    if not os.path.exists(test_folder):
        os.mkdir(test_folder)

    output_path = os.path.join(test_folder,TEST_DATA_IN1)
    expected_path = os.path.join(test_folder,TEST_DATA_OUT)
    with open(output_path,'w',encoding="utf8") as output_file,\
    open(expected_path,'w',encoding="utf8") as expected_file:
        out_writer = csv.writer(output_file, quoting = csv.QUOTE_NONE, lineterminator='\n')
        exp_writer = csv.writer(expected_file, quoting = csv.QUOTE_NONE, lineterminator='\n')
        out_writer.writerow(CURRENT_HEADER)
        exp_writer.writerow(RESULT_HEADER)
        for i in range(1,MAX_CURRENT+1):
            if 1 <= i <= 100000:
                data_row=[i, 'No','No', 'Yes', today]
                expected_row=[i, 'No','No', 'Yes', today,84,42,0]
            if 100001 <= i <= 200000:
                data_row=[i, 'No','Yes', 'No', today]
                expected_row=[i, 'No','Yes', 'No', today,0,84,0]
            if 200001 <= i <= 300000:
                data_row=[i, 'No','Yes', 'Yes', today]
                expected_row=[i, 'No','Yes', 'Yes', today,84,42,0]
            out_writer.writerow(data_row)
            exp_writer.writerow(expected_row)

def generate_history(test_name,days):
    """
    Generate Volume based Data sets where:
    300000 current records or keys
    25000000+ Historical records
    keys 1-100000
     - a: Yes on MIN(date) - only first date in historical data range
     - b: Yes on MID(date) - mid historical data range
     - c: Yes on MAX(date) - current
    keys 100001-200000
     - a: Never Yes
     - b: Always Yes
     - c: Never Yes
    keys 200001-300000
     - a: Yes Before MID(date)
     - b: Yes ON or After MID(date)
     - c: Yes on MAX(date)
    """
    test_folder=os.path.join(TEST_BASE_FOLDER,"dynamic-data",test_name)
    if not os.path.exists(test_folder):
        os.mkdir(test_folder)

    output_path = os.path.join(test_folder,TEST_DATA_IN2)
    with open(output_path,"w",encoding="utf8") as output_file:
        writer = csv.writer(output_file,quoting = csv.QUOTE_NONE, lineterminator='\n')
        writer.writerow(HISTORY_HEADER)
        num_rows = 0
        for day_offset in range(days,-1,-1):
            historical_date=(datetime.now() - timedelta(day_offset)).strftime("%m/%d/%Y")
            for i in range(1,MAX_CURRENT+1):
                if 1 <= i <= 100000:
                    attr_a = "Yes" if day_offset == days else "No"
                    attr_b = "Yes" if day_offset == round(days/2,0) else "No"
                    attr_c = "Yes" if day_offset == 0 else "No"
                if 100001 <= i <= 200000:
                    attr_a = "No"
                    attr_b = "Yes"
                    attr_c = "No"
                if 200001 <= i <= 300000:
                    attr_a = "Yes" if day_offset > round(days/2,0) else "No"
                    attr_b = "Yes" if day_offset <= round(days/2,0) else "No"
                    attr_c = "Yes" if day_offset == 0 else "No"
                data_row=[i, attr_a, attr_b, attr_c, historical_date]
                writer.writerow(data_row)
                num_rows += 1

    print(f"Created historical data file {test_name} with {num_rows} data rows.")

# MAIN

print("NEXT: Setup test folders")
generate_folder_structure()

print("NEXT: Setup sample data")
generate_sample()
generate_min_mid_max()

print("NEXT: Setup volume data")
start_time = datetime.now()
print(f'StartTime {start_time}')
generate_current("max_history")
generate_history("max_history",84)
time_elapsed = datetime.now() - start_time
print(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

print("Done")
