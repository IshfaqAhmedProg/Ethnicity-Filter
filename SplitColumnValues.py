import os
from traceback import format_exc

from pandas import read_csv
from pypeepa import (
    getFilePath,
    processCSVInChunks,
    initLogging,
    loggingHandler,
    listDir,
    createDirectory,
    askYNQuestion,
    printArray,
    selectOptionQuestion,
    # concurrentFutures, # For multiprocessing
    ProgressSaver,
)
import time


def splitName(s):
    parts = str(s).split(" ")
    firstName = parts[0]
    lastName = parts[-1]
    middleName = " ".join(parts[1:-1])
    return firstName, middleName, lastName


def splitAddress(s):
    parts = str(s).rsplit(" ", 1)  # Split only once from the right
    city = ""
    state = ""
    country = ""
    zip_code = ""

    if len(parts) == 2:
        address_parts = parts[0].rsplit(" ", 2)
        if len(address_parts) >= 1:
            city = address_parts[0]
        if len(address_parts) >= 2:
            # Check if the second-to-last part is a valid state code
            if address_parts[-2].isalpha() and len(address_parts[-2]) <= 3:
                state = address_parts[-2]
                country = address_parts[-1]
            else:
                country = address_parts[-2]
                state = address_parts[-1]
        zip_code = parts[1]

    return city, state, country, zip_code


# Define the splitColumn function
def splitColumnValues(chunk, config):
    # Apply the split_string function to the 'address' column
    if config["split_address_column"] != None:
        chunk[config["address_columns"]] = (
            chunk[config["split_address_column"]].apply(splitAddress).to_list()
        )
        # Drop the original 'address' column
        chunk.drop(columns=[config["split_address_column"]], inplace=True)

    # Apply the split_string function to the 'name' column
    if config["split_name_column"] != None:
        chunk[config["name_columns"]] = (
            chunk[config["split_name_column"]].apply(splitName).to_list()
        )
        # Drop the original 'name' column
        chunk.drop(columns=[config["split_name_column"]], inplace=True)

    # Rename fucked up header names
    new_column_names = {"t": "phoneNumber", "dob": "dateOfBirth", "e": "email"}
    chunk.rename(columns=new_column_names, inplace=True)

    return chunk


# Main function
# variables:
# D:\Downloads\MGM_Grand_Hotels1
async def main():
    app_name = "SplitColumnValues"
    # Initialising Logger
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

    # Initialise progress saver
    progress = ProgressSaver(app_name)

    # If saved_data length more than 0 ask users if they want to continue previous process
    if len(progress.saved_data) > 0:
        continue_from_before = askYNQuestion("Continue from before?(y/n)")
        if not continue_from_before:
            progress.resetSavedData(logger)

    # Get the list of input directory files.
    input_files = listDir(input_dir)

    split_names = askYNQuestion("Do you want to split names?(y/n)")
    split_address = askYNQuestion("Do you want to split address?(y/n)")
    file_columns_same = askYNQuestion(
        "Are all the column names the same for all the files in the input dir?(y/n)"
    )
    # ##### Multiprocessing ##### Start the processing input directory files
    # concurrentFutures(
    #     process_function=taskProcessor,
    #     divided_tasks=input_files,
    #     additional_args=(
    #         input_dir,
    #         split_column_config,
    #         chunk_size,
    #         output_dir,
    #         logger,
    #         progress,
    #     ),
    #     max_workers=4,
    #     logger=logger,
    # )

    #### No multiprocessing ##### Start the process on each input directory files
    tick = time.time()
    count = 0
    for input_file in input_files:
        task_tick = time.time()
        input_full_path = os.path.join(input_dir, input_file)
        if input_full_path not in progress.saved_data:
            try:
                if not file_columns_same or count == 0:
                    columns = read_csv(input_full_path, nrows=1).columns
                    address_index = names_index = None
                    printArray(columns)
                    if split_names:
                        names_index = selectOptionQuestion(
                            question=f"Enter the index of the column containing the names.",
                            min=1,
                            max=len(columns),
                        )
                    if split_address:
                        address_index = selectOptionQuestion(
                            question=f"Enter the index of the column containing the addresses.",
                            min=1,
                            max=len(columns),
                        )
                split_column_config = {
                    "address_columns": ["city", "state", "country", "postalCode"],
                    "split_address_column": None
                    if not split_address
                    else columns[int(address_index) - 1],
                    "name_columns": ["firstName", "middleName", "lastName"],
                    "split_name_column": None
                    if not split_names
                    else columns[int(names_index) - 1],
                }
                df = processCSVInChunks(
                    input_full_path,
                    splitColumnValues,
                    split_column_config,
                    chunk_size,
                )
                # Output the file to output folder with same name.
                output_path = os.path.join(output_dir, input_file)
                df.to_csv(output_path, index=False)
                loggingHandler(
                    logger,
                    f"Results for {input_file}, Time taken:{time.time()-task_tick}s -> {output_path}",
                )
                # Add to the completed files list
                progress.saveToJSON(input_full_path, input_full_path, logger)
                count += 1
            except Exception as err:
                traceback_info = format_exc()
                # Log the exception message along with the traceback information
                log_message = (
                    f"Exception occurred: {str(err)}\nTraceback:\n{traceback_info}"
                )
                loggingHandler(logger, log_message)
        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {input_full_path}"
            )
    loggingHandler(
        logger,
        f"Total time taken:{time.time()-tick}s",
    )


# def taskProcessor(
#     input_file: str,
#     input_dir: str,
#     split_column_config,
#     chunk_size: int,
#     output_dir: str,
#     logger: Logger,
#     progress: ProgressSaver,
# ):
#     input_full_path = os.path.join(input_dir, input_file)
#     if input_full_path not in progress.saved_data:
#         try:
#             df = processCSVInChunks(
#                 input_full_path,
#                 splitColumn,
#                 split_column_config,
#                 chunk_size,
#             )
#             # Output the file to output folder with same name.
#             output_path = os.path.join(output_dir, input_file)
#             df.to_csv(output_path, index=False)
#             loggingHandler(logger, f"Results for {input_file} -> {output_path}")
#             # Add to the completed files list
#             progress.saveToJSON(input_full_path, input_full_path, logger)

#         except Exception as err:
#             logger.exception("Exception Occurred:  " + str(err))
#     else:
#         loggingHandler(
#             logger, f"Skipping file as already complete -> {input_full_path}"
#         )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
