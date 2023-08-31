# Large Dataset Tools

### 1. SplitCSV

   Split CSV allows you to split one csv to multiple csvs, it has two modes that you can split in, **Column Values** and **Reference File Columns**.

   * #### Column Values : 

      Splits to multiple folders based on the value found in the row of the selected column. eg:- if input file "*input1.csv*" looks like this, and if you selected the column **occupation** then output will be: "output_dir/**Plumber**/input1.csv", "output_dir/**Carpenter**/input1.csv"

            Inputs:
               input1.csv                                               
                     | names | address | email      | occupation |
                     | ----- | ------- | ---------- | ---------- |
                     | foo   | foobar  | foo@bar.fo | Plumber    |
                     | faa   | faabar  | faa@bar.fa | Carpenter  |
                     | fo    | fobar   | fo@bar.fo  | Plumber    |
                     
            Outputs:                   
               output_dir/Plumber/input1.csv                                               
                     | names | address | email      | occupation |
                     | ----- | ------- | ---------- | ---------- |
                     | foo   | foobar  | foo@bar.fo | Plumber    |
                     | fo    | fobar   | fo@bar.fo  | Plumber    |
               
               output_dir/Carpenter/input1.csv                                               
                     | names | address | email      | occupation |
                     | ----- | ------- | ---------- | ---------- |
                     | faa   | faabar  | faa@bar.fa | Carpenter  |
   
   * #### Reference File Columns : 
      
      **Requires a reference file which has values to match*.

      Splits to multiple folders based on the column name in the reference file. eg:- if the reference file looks like this, and the input file and selected column is the same as the example in `Column Values`, then the output will be: "output_dir/**spanishNames**/input1.csv", "output_dir/**englishNames**/input1.csv"

            Inputs:
               reference file:
                     | spanishNames | englishNames |
                     | ------------ | ------------ |
                     | foo          | ho           |
                     | faa          | hum          |
                     | hee          | fo           |
               
               input1.csv                                               
                     | names | address | email      | occupation |
                     | ----- | ------- | ---------- | ---------- |
                     | foo   | foobar  | foo@bar.fo | Plumber    |
                     | faa   | faabar  | faa@bar.fa | Carpenter  |
                     | fo    | fobar   | fo@bar.fo  | Plumber    |
                     
            Outputs:
               output_dir/spanishNames/input1.csv                                              
                     | names | address | email      | occupation |
                     | ----- | ------- | ---------- | ---------- |
                     | foo   | foobar  | foo@bar.fo | Plumber    |
                     | faa   | faabar  | faa@bar.fa | Carpenter  |
                     
               output_dir/englishNames/input1.csv                                              
                     | names | address | email     | occupation |
                     | ----- | ------- | --------- | ---------- |
                     | fo    | fobar   | fo@bar.fo | Plumber    |
                     
### 2. SplitToMultipleColumns

   Split to Multiple Columns allows you to split one column based on the type of the value to multiple columns. The types of split possible are:

   * #### Name type value :
      
       Converts values like `{name : "John H. Doe"}` to 3 columns, `{firstName:"John", middleName: "H.", lastName:"Doe"}`

   * #### Address type value : 
   
      Converts values like `{address : "Boron CA US 93516"}` to 4 columns, `{city : "Boron", state : "CA", country : "US", zip : "93516"}`

### 3. JoinMultipleCSV

   Join multiple CSV files based on the column provided, Before running this, make sure you have the files you want to join to (***left***) in one folder, and the files you want to join with (***right***) in another folder.
   The ***left*** will be kept as is and only the new columns from the ***right*** will be added to it. You can have multiple ***left***  and ***right*** files, with completely different column headers.   

### 4. FilterACN

   Filter out the age country or names from a csv file ***WIP***

### 5. RemoveColumns

   Remove columns that you need to specify on a .json file 
   
   eg:- If input file looks like this,

               Inputs:
               input1.csv                                               
                     | names | address | email      | occupation |
                     | ----- | ------- | ---------- | ---------- |
                     | foo   | foobar  | foo@bar.fo | Plumber    |
                     | faa   | faabar  | faa@bar.fa | Carpenter  |
                     | fo    | fobar   | fo@bar.fo  | Plumber    |

   
   and you want to remove the "address" and "occupation", then the .json file should look like this `[ 'address', 'occupation' ]`.

               Outputs:
               output_dir/input1.csv                                               
                     | names | email      |
                     | ----- | ---------- |
                     | foo   | foo@bar.fo |
                     | faa   | faa@bar.fa |
                     | fo    | fo@bar.fo  |

   **Make sure to check for commas and quotes in the .json file*.

### 6. RemoveNullValues

   Remove null values from columns that you need to specify on a .json file, for multiple columns, the row is removed only when all of the specified column values are null 
   
   eg:- If input file looks like this,

               Inputs:
               input1.csv                                               
                     | names | address | email      | occupation |
                     | ----- | ------- | ---------- | ---------- |
                     | foo   | foobar  | foo@bar.fo |            |
                     | faa   | faabar  |            | Carpenter  |
                     | fo    | fobar   |            |            |

   
   and you want to remove the null values in "email" and "occupation", then the .json file should look like this `[ 'email', 'occupation' ]`.
   
               Outputs:
               output_dir/input1.csv                                               
                     | names | address | email      | occupation |
                     | ----- | ------- | ---------- | ---------- |
                     | foo   | foobar  | foo@bar.fo |            |
                     | faa   | faabar  |            | Carpenter  |

   **Make sure to check for commas and quotes in the .json file*.


### 7. ReorderColumns

   Reorder the columns of any csv files based on the columns specified on the .json file, The order of the array in the .json file is the order of the columns on the output file. Any column not specified will be dropped.

   eg:- If input file looks like this,

               Inputs:
               input1.csv                                               
                     | names | address | email      | occupation |
                     | ----- | ------- | ---------- | ---------- |
                     | foo   | foobar  | foo@bar.fo | Plumber    |
                     | faa   | faabar  | faa@bar.fa | Carpenter  |
                     | fo    | fobar   | fo@bar.fo  | Plumber    |

   and you want to reorder it as "email" first, then the .json file should look like this `[ 'email', 'name', 'address', 'occupation' ]`.

               Outputs:
               output_dir/input1.csv                                               
                     | email      | names  | address | occupation |
                     | ---------- | ------ | ------- | ---------- |
                     | foo@bar.fo | foobar | foo     | Plumber    |
                     | faa@bar.fa | faabar | faa     | Carpenter  |
                     | fo@bar.fo  | fobar  | fo      | Plumber    |