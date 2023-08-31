import chardet
import csv
import os
from pypeepa import initLogging, getFilePath, listDir, createDirectory, loggingHandler


def convertLargeCSVToUTF8(file_path, output_path, logger, chunk_size=8192):
    with open(file_path, "rb") as f:
        raw_data = f.read()
        encoding_info = chardet.detect(raw_data)
        original_encoding = encoding_info["encoding"]

    with open(
        file_path, "r", encoding=original_encoding, errors="replace"
    ) as f_read, open(output_path, "w", encoding="utf-8") as f_write:
        csv_reader = csv.reader(f_read)
        csv_writer = csv.writer(f_write)

        while True:
            chunk = f_read.read(chunk_size)
            if not chunk:
                break

            csv_reader = csv.reader(chunk.splitlines())
            for row in csv_reader:
                csv_writer.writerow(row)

    loggingHandler(logger, f"Converted {file_path} from {original_encoding} to UTF-8.")


async def main():
    app_name = "ChangeEncoding"
    # Initialising logging
    logger = initLogging(app_name)

    # User inputs
    input_dir = getFilePath(
        "Enter the location of the folders containing the files to convert: ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )
    # Get the folders from the input_dir
    input_dir_content = listDir(input_dir)

    # Create outputdirectory if doesnt exist
    createDirectory(output_dir)

    for input_file in input_dir_content:
        input_path = os.path.join(input_dir, input_file)
        output_file_path = os.path.join(output_dir, f"{input_file}.csv")
        convertLargeCSVToUTF8(input_path, output_file_path, logger, 10000)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
