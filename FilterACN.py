import os
from pypeepa import (
    ProgressSaver,
    getFilePath,
    processCSVInChunks,
    initLogging,
    loggingHandler,
    createDirectory,
    askYNQuestion,
    readJSON,
    listDir,
    printArray,
    selectOptionQuestion,
)
from datetime import datetime
import time
from pandas import read_csv


def calculate_age(dob_parts, current_year):
    try:
        birth_year = int(dob_parts[0])
        age = current_year - birth_year
        return age
    except ValueError:
        return -1  # Return a default value for NaN or invalid values


def filterDataframeByAgeCountryAndNames(chunk, process_config):
    # Specify date format and use 'errors' parameter to handle invalid dates
    dob_parts = chunk[process_config["dob_column"]].str.split("/", expand=True)
    chunk["age"] = dob_parts.apply(
        calculate_age, args=(process_config["current_year"],), axis=1
    )

    # Perform a loose name matching
    common_names_lower = [name.lower() for name in process_config["common_names"]]
    name_matches = (
        chunk[process_config["names_header"]].str.lower().isin(common_names_lower)
    )

    filtered_df = chunk[
        (
            chunk[process_config["countries_header"]].isin(
                process_config["valid_countries"]
            )
        )
        & (chunk["age"] > process_config["age_value"])
        & name_matches
    ]

    return filtered_df


async def main():
    app_name = "FilterACN"
    logger = initLogging(app_name)
    # User inputs
    input_dir = getFilePath(
        "Enter the input files location: ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )
    createDirectory(output_dir)
    reference_csv_file_path = getFilePath(
        "Enter the file containing the common names: ", (".json"), False
    )
    chunk_size = 100000
    # Example usage
    current_year = datetime.now().year

    # Load common male names from the reference CSV
    # Read original CSV file into a pandas DataFrame in chunks
    valid_countries = ["US", "USA"]  # List of valid country values

    progress = ProgressSaver(f"completedFiles-{app_name}")
    saved_data = progress.initialiseJSONSaver()

    # If saved_data length more than 0 ask users if they want to continue previous process
    if len(saved_data) > 0:
        continue_from_before = askYNQuestion("Continue from before?(y/n)")
        if not continue_from_before:
            progress.resetSavedData(logger)
    # Get the list of input directory files.
    input_files = listDir(input_dir, "files")

    common_names = readJSON(reference_csv_file_path)

    filter_age = askYNQuestion("Do you want to filter age?(y/n)")
    filter_countries = askYNQuestion("Do you want to filter country?(y/n)")
    filter_names = askYNQuestion("Do you want to filter names?(y/n)")
    file_columns_same = askYNQuestion(
        "Are all the column names the same for all the files in the input dir?(y/n)"
    )
    # Start the process on each input directory files
    tick = time.time()
    first_file = True
    for input_file in input_files:
        task_tick = time.time()
        input_full_path = os.path.join(input_dir, input_file)
        if input_full_path not in progress.saved_data:
            try:
                if not file_columns_same or first_file:
                    # Get the first line for header names
                    all_columns = read_csv(input_full_path, nrows=1).columns
                    dob_index = countries_index = names_index = None
                    printArray(all_columns)
                    if filter_age:
                        dob_index = selectOptionQuestion(
                            question=f"Enter the index of the column containing the date of births.",
                            min=1,
                            max=len(all_columns),
                        )
                        age = selectOptionQuestion(
                            question=f"Enter the age to filter, will include provided age and older",
                            min=1,
                            max=120,
                        )
                    if filter_countries:
                        countries_index = selectOptionQuestion(
                            question=f"Enter the index of the column containing the country.",
                            min=1,
                            max=len(all_columns),
                        )
                        valid_countries = ["US", "USA"]  # List of valid country values
                    if filter_names:
                        names_index = selectOptionQuestion(
                            question=f"Enter the index of the column containing the country.",
                            min=1,
                            max=len(all_columns),
                        )
                process_config = {
                    "current_year": current_year,
                    "common_names": common_names,
                    "dob_column": None if filter_age else all_columns[dob_index - 1],
                    "age_value": age,
                    "names_header": None
                    if filter_names
                    else all_columns[names_index - 1],
                    "countries_header": None
                    if filter_countries
                    else all_columns[countries_index - 1],
                    "valid_countries": valid_countries,
                }
                df = processCSVInChunks(
                    input_full_path,
                    filterDataframeByAgeCountryAndNames,
                    process_config,
                    chunk_size,
                )
                # Output the file to output folder with same name as input file.
                output_path = os.path.join(output_dir, input_file)
                df.to_csv(output_path, index=False)
                loggingHandler(
                    logger,
                    f"Results for {input_file}, Time taken:{time.time()-task_tick}s -> {output_path}",
                )

                # Add to the completed files list
                progress.saveToJSON(input_full_path, input_file, logger)
                first_file = False

            except Exception as err:
                loggingHandler(logger, f"Exception Occurred:  {str(err)}")

        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {input_full_path}"
            )
    loggingHandler(
        logger,
        f"Total time taken:{time.time()-tick}s",
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
