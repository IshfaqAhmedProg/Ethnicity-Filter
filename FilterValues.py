import os
import time
from dateparser import parse
from datetime import datetime
from traceback import format_exc
from pandas import read_csv, DataFrame
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
    askHeaderForMultipleCSV,
)


def calculate_age(date_str, current_year):
    dob = parse(date_str)
    if dob:
        age = current_year - dob.year
        return age
    return -1  # Return a default value for NaN or invalid values


def filterDataFrameByAgeAndCommonValues(chunk: DataFrame, process_config: dict):
    """
    Filter a DataFrame based on age and common values.
    This function filters a given DataFrame based on the specified age and common values.

    @param:`chunk`: The DataFrame to be filtered.
    @param:`process_config`: Dictionary containing configurations for filtering.\n
            @key:`current_year` (int or None): Current year used for age calculation.\n
            @key:`dob_column` (str or None): Column name containing date of birth.\n
            @key:`age_value` (int or None): Minimum age for filtering.\n
            @key:`common_values` (list or None): List of common values to filter by.\n
            @key:`common_value_header` (str or None): Column header for common values.\n
            @key:`reverse_filter` (boolean or False): Remove the match and keep the non match
    @return:
        Filtered DataFrame containing rows that satisfy the age and common value criteria.
    """
    (
        current_year,
        dob_column,
        age_value,
        common_values,
        common_value_header,
        reverse_filter,
    ) = process_config.values()
    # Setting all to true so that and logic can work so if multiple filters are chosen when both are satisfied then keep those only
    value_matches = filter_age = True
    # Filter age
    if dob_column != None:
        chunk[dob_column] = chunk[dob_column].astype(
            str
        )  # Ensure the column is of string type
        chunk["age"] = chunk[dob_column].apply(calculate_age, args=(current_year,))
        filter_age = chunk["age"] > age_value

    # Filter values
    if common_value_header is not None:
        common_values_lower = [name.lower() for name in common_values]
        value_matches = (
            chunk[common_value_header].str.lower().isin(common_values_lower)
            if not reverse_filter
            else ~chunk[common_value_header].str.lower().isin(common_values_lower)
        )

    filtered_df = chunk[filter_age & value_matches]

    return filtered_df


async def main():
    app_name = "FilterValues"
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
    filter_values = askYNQuestion("Do you want to filter values?(y/n)")
    file_columns_same = askYNQuestion(
        "Are all the column names the same for all the files in the input dir?(y/n)"
    )
    reverse_filter = askYNQuestion(
        "Reverse filter: Remove the match and keep the non match?(y/n)"
    )
    # TODO Use this for loop to iterate files so that user doesnt have to keep repeating header values for each file
    # input_files_value_headers=askHeaderForMultipleCSV(input_files, input_dir,"values")
    # input_files_dob_headers=askHeaderForMultipleCSV(input_files, input_dir,"dob")
    # for (value_header, filename), (dob_header, _) in zip(input_files_value_headers, input_files_dob_headers):
    #     print(f'String 1: {value_header}, Filename: {filename}')
    #     print(f'String 2: {dob_header}, Filename: {filename}')

    # Start the process on each input directory files
    tick = time.time()
    first_file = True
    for input_file in input_files:
        task_tick = time.time()
        input_full_path = os.path.join(input_dir, input_file)
        if input_full_path not in progress.saved_data and (filter_values or filter_age):
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
                            "Enter the file containing the common values: ",
                            (".json"),
                            False,
                        )
                        common_values = readJSON(reference_json_file_path)

                process_config = {
                    "current_year": None if not filter_age else current_year,
                    "dob_column": None
                    if not filter_age
                    else all_columns[dob_index - 1],
                    "age_value": None if not filter_age else age,
                    "common_values": None if not filter_values else common_values,
                    "common_value_header": None
                    if not filter_values
                    else all_columns[names_index - 1],
                    "reverse_filter": reverse_filter,
                }
                df = processCSVInChunks(
                    input_full_path,
                    filterDataFrameByAgeAndCommonValues,
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
                traceback = format_exc()
                loggingHandler(
                    logger,
                    f"Exception occurred: {str(err)}\n{traceback}",
                )

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
