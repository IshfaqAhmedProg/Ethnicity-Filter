# Instructions

### SplitToMultipleCSV
1. Open command prompt on the folder containing the scripts, and type:  ` pip install -r requirements.txt `.
2. Check if the **appConfig-SplitToMultipleCSV.json** has all the configs you desire.
   - **column_to_split**: This is the column which will be split to multiple csv files based on the value in a row.
   - **check_for_null**: This is a list of the columns you want to check if they have null values or not.
   - **ordered_columns**: This is the order of the columns in the final output. Keep empty if you dont want to change the original
   - **delete_columns**: These are the columns that will be removed. Keep empty if you want to keep all columns.
3. Then run:  ` python SplitToMultipleCSV.py `  and follow the instructions.
4. Make sure the output and the input folders are in the same drive when prompted.
5. Once the **SplitToMultipleCSV** script completes, run : ` python AppendCSVFiles.py ` and follow the instructions.
6. The input in **AppendCSVFiles** will be the output of **SplitToMultipleCSV**.
7. If any error occurs send me the *.log files that will be produced.

