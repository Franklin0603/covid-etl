#!/bin/bash

datasource='https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
csv_file='/data/raw_data/covid-raw.csv'

function run_help() {
    echo "Usage: ./script.sh [flag_command]"
    echo "Flag commands:"
    echo "run_validate     - Indicate that data should be validated."
    echo "run_localsource  - Indicate that the source is local."
    echo "pip_install      - Install the Python dependencies."
    echo "help             - Show this help message."
}

function run_python_script() {
    flag_command=$1

    case $flag_command in
        "run_validate")
            python main.py nyt_cases_counties ${datasource} --validate-data
            ;;
        "run_localsource")
            python main.py nyt_cases_counties ${csv_file} --local-source
            ;;
        "pip_install")
            python3 -m pip install --upgrade pip -r requirements.txt
            ;;
        "help")
            run_help
            ;;
        *)
            echo "Invalid command. Run './script.sh help' for usage information."
            exit 1
            ;;
    esac
}

# check if the correct number of arguments are provided
if [[ $# -ne 1 ]]; then
    run_help
    exit 1
fi

run_python_script $1
