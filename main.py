import sys
from utils import json_utils, spark_utils


base_path = "files"

if __name__ == "__main__":
    
    # get dataflows from json and create dataframes from input files
    dataflows = json_utils.get_dataflows_from_json("metadata.json")

    # get parameter from command line
    if len(sys.argv) > 1:
        year = sys.argv[1]
    else:
        year = "2025"

    #apply transformations and export to json files and delta tables
    spark_utils.process_dataflows(dataflows, year)
