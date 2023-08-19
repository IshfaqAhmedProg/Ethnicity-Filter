# Instructions

1. Open command prompt on the folder containing the scripts, and type:  ` pip install -r requirements.txt `.
2. Check if the **orderedColumns.json** has all the columns you desire.
3. Then run:  ` python SplitByEthnicity.py `  and follow the instructions.
4. Make sure the output and the input folders are in the same drive when prompted.
5. Once the **SplitByEthnicity** script completes, run : ` python AppendCSVFiles.py ` and follow the instructions.
6. The input in **AppendCSVFiles** will be the output of **SplitByEthnicity**.
7. If any error occurs send me the *.log files that will be produced.

### Timings
- Time calculated for ~1250 parts: ~4.2 Hrs
   - Per Part : 12s Avg