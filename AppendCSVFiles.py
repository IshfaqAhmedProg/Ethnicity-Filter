import os
import glob
import pandas as pd
import logging
from pypeepa.fileInteraction import getFilePath, asyncListDir, createDirectory
from pypeepa.userInteraction import progressBarIterator


async def appendCSVFiles(directory_path, output_file_path, chunk_size=1000):
    # Find all CSV files in the given directory
    csv_files = glob.glob(os.path.join(directory_path, "*.csv"))

    if not csv_files:
        print("No CSV files found in the directory.")
        return

    # Initialize the output file
    header_written = False

    # Loop through each CSV file and append its contents to the output file
    with open(output_file_path, "wb") as output_file:
        for csv_file in progressBarIterator(csv_files, f"Appending "):
            print(f"Processing {csv_file}")
            for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
                if not header_written:
                    chunk.to_csv(output_file, index=False, header=True, mode="wb")
                    header_written = True
                else:
                    chunk.to_csv(output_file, index=False, header=False, mode="ab")

    print(f"Combined data saved to '{output_file_path}'.")


async def main():
    # Initialising logging
    logging.basicConfig(
        filename="ExceptionLogs-AppendCSVFiles.log",
        format="%(asctime)s %(message)s",
    )
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Setting the threshold of logger to DEBUG
    logger.debug("------------------App initialised!------------------")

    # User inputs
    input_dir = await getFilePath(
        "Enter the location of the folders containing the files to combine: ",
    )
    output_dir = await getFilePath(
        "Enter the output location: ",
    )
    # Get the folders from the input_dir
    input_dir_content = await asyncListDir(input_dir, "folders")

    # Create outputdirectory if doesnt exist
    await createDirectory(output_dir)

    for dir in input_dir_content:
        input_path = os.path.join(input_dir, dir)
        output_file_path = os.path.join(output_dir, f"{dir}.csv")
        await appendCSVFiles(input_path, output_file_path, 10000)
        print(dir)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
