import os
import pandas as pd
import logging
import json
from alive_progress import alive_bar

from pypeepa import (
    initLogging,
    createDirectory,
    getFilePath,
    asyncListDir,
    asyncReadJSON,
    loggingHandler,
)


async def splitByEthnicity(chunk, props):
    for ind_ethnic_code, group in chunk.groupby(props["column_to_split"]):
        ind_output_dir = os.path.join(
            props["output_dir"],
            ind_ethnic_code,
        )
        # Create output folder if it doesn't exist
        await createDirectory(ind_output_dir)
        output_file = os.path.join(
            ind_output_dir,
            props["input_file"],
        )

        # Remove specified columns from the group
        columns_to_drop = [
            col for col in props["delete_columns"] if col in group.columns
        ]
        group_filtered = group.drop(columns=columns_to_drop, errors="ignore")

        # Filter rows based on non-empty values in specified columns
        for col in props["check_for_null"]:
            if col in group.columns:
                group = group[group[col].notnull()]

        # Reorder columns based on a predefined order
        ordered_columns = props["ordered_columns"]
        group_filtered = group.reindex(columns=ordered_columns, fill_value="")

        # Output the file to its destination
        group_filtered.to_csv(
            output_file, mode="a", header=not os.path.exists(output_file), index=False
        )


# Main function
# variables:
async def main():
    # Initialising logging
    logger = await initLogging("SplitByEthnicity")
    # User inputs
    input_dir = await getFilePath(
        "Enter the input files location: ",
    )
    output_dir = await getFilePath(
        "Enter the output location: ",
    )
    completed_files_name = "completedFiles.json"

    chunk_size = 10000

    # Reading completed files data
    completed_files = await asyncReadJSON(completed_files_name)

    app_config = await asyncReadJSON("appConfig.json")
    # Get the list of input directory files.
    input_files = await asyncListDir(input_dir, "files")

    print("Files detected in input folder:", len(input_files))

    # Start the process on each input directory files
    for input_file in input_files:
        input_full_path = os.path.join(input_dir, input_file)
        if input_full_path not in completed_files:
            process_config = {
                "output_dir": output_dir,
                "input_file": input_file,
                "delete_columns": app_config["delete_columns"],
                "check_for_null": app_config["check_for_null"],
                "ordered_columns": app_config["ordered_columns"],
                "column_to_split": app_config["column_to_split"],
            }
            try:
                chunk_reader = pd.read_csv(
                    input_full_path, chunksize=chunk_size, low_memory=False
                )
                # Count total chunks for progress bar
                total_chunks = int(
                    sum(1 for row in open(input_full_path, "r")) / chunk_size
                )
                # Show progress bar and run main process
                with alive_bar(
                    total_chunks, force_tty=True, bar="filling", spinner="waves"
                ) as bar:
                    bar.title = "Processing file -> " + input_file
                    for chunk in chunk_reader:
                        # The main process
                        await splitByEthnicity(chunk, process_config)
                        bar()

                # Output the file to output folder with same name.
                output_path = os.path.join(output_dir, input_file)
                loggingHandler(logger, f"Results for {input_file} -> {output_path}")

                # Add to the completed files list
                completed_files.append(input_full_path)
                with open(completed_files_name, "w+") as completed_output:
                    loggingHandler(logger, f"Saving to completed files list.")
                    json.dump(completed_files, completed_output)

            except Exception as err:
                loggingHandler(logger, f"Exception Occurred: { str(err)} ")
        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {input_full_path}"
            )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
