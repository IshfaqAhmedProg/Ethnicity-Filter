import os
import pandas as pd
import logging
import json
from alive_progress import alive_bar
from pypeepa.fileInteraction import (
    createDirectory,
    getFilePath,
    asyncListDir,
    asyncReadJSON,
)


async def splitByEthnicity(chunk, props):
    for ind_ethnic_code, group in chunk.groupby("Ind_Ethnic_Code"):
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
    logging.basicConfig(
        filename="ExceptionLogs-SplitByEthnicity.log",
        format="%(asctime)s %(message)s",
    )
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Setting the threshold of logger to DEBUG
    logger.debug("------------------App initialised!------------------")

    # User inputs
    input_dir = await getFilePath(
        "Enter the input files location: ",
    )
    output_dir = await getFilePath(
        "Enter the output location: ",
    )

    chunk_size = 10000
    completed_files_name = "completedFiles.json"
    delete_columns_file_name = "deleteColumns.json"
    ordered_columns_file_name = "orderedColumns.json"
    check_for_null = ["Email_Present_Flag", "Phone"]

    # Reading completed files
    completed_files = await asyncReadJSON(completed_files_name)

    # Reading orderedColumns files from json file
    ordered_columns = await asyncReadJSON(ordered_columns_file_name)

    # Reading deleted columns
    delete_columns = await asyncReadJSON(delete_columns_file_name)

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
                "delete_columns": delete_columns,
                "check_for_null": check_for_null,
                "ordered_columns": ordered_columns,
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

                print(f"Writing to file -> {output_path}")

                logger.debug(f"Results for {input_file} -> {output_path}")

                # Add to the completed files list
                completed_files.append(input_full_path)
                with open(completed_files_name, "w+") as completed_output:
                    logger.debug(f"Adding to completed files list -> {completed_files}")
                    json.dump(completed_files, completed_output)

            except Exception as err:
                logger.exception("Exception Occurred:  " + str(err))
        else:
            skip_mssg = f"Skipping file as already complete -> {input_full_path}"
            logger.debug(skip_mssg)
            print(skip_mssg)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
