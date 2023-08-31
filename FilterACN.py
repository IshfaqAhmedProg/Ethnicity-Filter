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
    askSelectOptionQuestion,
)
from datetime import datetime
import time
from pandas import read_csv, DataFrame


def calculate_age(dob_parts, current_year):
    try:
        birth_year = int(dob_parts[0])
        age = current_year - birth_year
        return age
    except ValueError:
        return -1  # Return a default value for NaN or invalid values


def filterDataframeByAgeCountryAndNames(chunk: DataFrame, process_config: dict):
    # "current_year":
    # "common_values":
    # "dob_column":
    # "age_value":
    # "common_value_header":
    # "countries_header":
    # "valid_countries":
    (
        current_year,
        common_values,
        dob_column,
        age_value,
        common_value_header,
    ) = process_config.values()

    # Setting all to true so that and logic can work so if multiple filters are chosen when both are satisfied then keep those only
    value_matches = filter_age = True
    # Filter age
    if dob_column != None:
        dob_parts = chunk[dob_column].str.split("/", expand=True)
        chunk["age"] = dob_parts.apply(calculate_age, args=(current_year,), axis=1)
    filter_age = chunk["age"] > age_value

    # Filter values
    common_values_lower = [name.lower() for name in common_values]
    value_matches = chunk[common_value_header].str.lower().isin(common_values_lower)

    filtered_df = chunk[filter_age & value_matches]

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

    chunk_size = 100000

    progress = ProgressSaver(app_name)

    # If saved_data length more than 0 ask users if they want to continue previous process
    progress.askToContinue(logger)
    # Get the list of input directory files.
    input_files = listDir(input_dir, "files")

    filter_age = askYNQuestion("Do you want to filter age?(y/n)")
    filter_values = askYNQuestion("Do you want to filter names?(y/n)")
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
                    dob_index = names_index = None
                    printArray(all_columns)
                    if filter_age:
                        dob_index = askSelectOptionQuestion(
                            question=f"Enter the index of the column containing the date of births.",
                            min=1,
                            max=len(all_columns),
                        )
                        age = askSelectOptionQuestion(
                            question=f"Enter the age to filter, output will include provided age and older",
                            min=1,
                            max=120,
                        )
                        current_year = datetime.now().year

                    if filter_values:
                        names_index = askSelectOptionQuestion(
                            question=f"Enter the index of the column containing the values you want to filter.",
                            min=1,
                            max=len(all_columns),
                        )
                        reference_json_file_path = getFilePath(
                            "Enter the file containing the common names: ",
                            (".json"),
                            False,
                        )
                        common_values = readJSON(reference_json_file_path)

                process_config = {
                    "current_year": current_year,
                    "common_values": common_values,
                    "dob_column": None if filter_age else all_columns[dob_index - 1],
                    "age_value": age,
                    "common_value_header": None
                    if filter_values
                    else all_columns[names_index - 1],
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
