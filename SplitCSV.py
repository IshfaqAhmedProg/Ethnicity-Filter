import os
import time
from typing import Any
from traceback import format_exc
from pandas import DataFrame, read_csv
from pypeepa import (
    initLogging,
    createDirectory,
    getFilePath,
    listDir,
    loggingHandler,
    askYNQuestion,
    processCSVInChunks,
    askSelectOptionQuestion,
    askHeaderForMultipleCSV,
    ProgressSaver,
)


def splitOnReferenceColumns(chunk: DataFrame, process_config: Any):
    for category, names_list in process_config["common_vals"].items():
        # Filter the data based on the names
        filtered_data = chunk[chunk[process_config["column_name"]].isin(names_list)]

        # Output only if there is filtered data
        if len(filtered_data.index) != 0:
            category_dir = os.path.join(process_config["output_dir"], category)
            createDirectory(category_dir)
            output_file = os.path.join(category_dir, process_config["input_file"])
            filtered_data.to_csv(
                output_file,
                mode="a",
                header=not os.path.exists(output_file),
                index=False,
            )
            loggingHandler(
                logger=process_config["logger"],
                log_mssg=f"Saved {len(filtered_data)} records to {output_file}",
            )


def splitOnColumnValues(df: DataFrame, props: Any):
    for ind_ethnic_code, group in df.groupby(props["column_to_split"]):
        ind_output_dir = os.path.join(
            props["output_dir"],
            str(ind_ethnic_code),
        )
        # Create output folder if it doesn't exist
        createDirectory(ind_output_dir)
        output_file = os.path.join(
            ind_output_dir,
            props["input_file"],
        )

        # Output the file to its destination
        group.to_csv(
            output_file, mode="a", header=not os.path.exists(output_file), index=False
        )


# Main function
# variables:
async def main():
    app_name = "SplitCSV"
    # Initialising logging
    logger = initLogging(app_name)

    # User inputs
    input_dir = getFilePath(
        "\nEnter the input files location: ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )
    chunk_size = 100000
    # Ask for split type and set selected type to the name
    split_types = [
        {
            "name": "Column Values",
            "desc": "Splits to multiple folders based on the value found in the row of the selected column",
        },
        {
            "name": "Reference File Columns",
            "desc": "Splits to multiple folder based on the column name in the reference file.\n    *Requires a reference file which has values to match.",
        },
    ]
    for idx, types in enumerate(split_types):
        print(f"\n[{idx+1}]=> {types['name']} :\n    {types['desc']}")
    split_type_index = askSelectOptionQuestion(
        "\nSelect the type of split you want to do on the input files",
        1,
        len(split_types),
    )
    selected_split_type = split_types[split_type_index - 1]["name"]
    loggingHandler(logger, f"Type Selected: {selected_split_type}")

    progress = ProgressSaver(app_name)
    # If saved_data length more than 0 ask users if they want to continue previous process
    if len(progress.saved_data) > 0:
        continue_from_before = askYNQuestion("\nContinue from before?(y/n)")
        if not continue_from_before:
            progress.resetSavedData(logger)

    # Get the list of input directory files.
    input_files = listDir(input_dir, get="files")
    input_files_and_headers = askHeaderForMultipleCSV(input_files, input_dir)

    # Final user inputs required for Reference File Columns split type
    if selected_split_type == "Reference File Columns":
        common_vals_path = getFilePath(
            message="\nEnter the path to the reference file containing the common values: ",
            endswith=tuple(".csv"),
            folder=False,
        )
        common_vals = read_csv(
            common_vals_path,
            low_memory=False,
            encoding_errors="ignore",
            on_bad_lines="skip",
        ).to_dict(orient="list")

    # Start the process on each input files and headers
    tick = time.time()
    for input_file_and_header in input_files_and_headers:
        task_tick = time.time()

        # Initialising Process for file
        input_full_path = input_file_and_header[0]
        input_col = input_file_and_header[1]
        input_file = os.path.basename(input_full_path)

        # Check saved_data for file
        if input_full_path not in progress.saved_data:
            try:
                # Switch based on the split type selected
                match selected_split_type:
                    case "Column Values":
                        process_config = {
                            "output_dir": output_dir,
                            "input_file": input_file,
                            "column_to_split": input_col,
                        }
                        processCSVInChunks(
                            csv_file=input_full_path,
                            process_function=splitOnColumnValues,
                            pf_args=process_config,
                            chunk_size=chunk_size,
                        )
                    case "Reference File Columns":
                        process_config = {
                            "output_dir": output_dir,
                            "input_file": input_file,
                            "common_vals": common_vals,
                            "column_name": input_col,
                            "logger": logger,
                        }
                        processCSVInChunks(
                            csv_file=input_full_path,
                            process_function=splitOnReferenceColumns,
                            pf_args=process_config,
                            chunk_size=chunk_size,
                        )
                loggingHandler(
                    logger,
                    f"Time taken to complete {input_file} -> {time.time()-task_tick}s",
                )
                # Add to the completed files list
                progress.saveToJSON(input_full_path, input_file, logger)

            except Exception as err:
                traceback_info = format_exc()
                # Log the exception message along with the traceback information
                loggingHandler(
                    logger,
                    f"Exception occurred: {str(err)}\nTraceback:\n{traceback_info}",
                )
        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {input_full_path}"
            )
    loggingHandler(logger, f"Time taken to complete all files -> {time.time()-tick}s")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
