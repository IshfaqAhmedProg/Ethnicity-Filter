# Large Dataset Tools

- [Large Dataset Tools](#large-dataset-tools)
  - [Introduction](#introduction)
  - [Split CSV](#split-csv)
  - [Split To Multiple Columns](#split-to-multiple-columns)
  - [Join Multiple CSV](#join-multiple-csv)
  - [Filter ACN](#filter-acn)
  - [Remove Columns](#remove-columns)
  - [Remove Null Values](#remove-null-values)
  - [Reorder Columns](#reorder-columns)
  - [Change Encoding](#change-encoding)

## Introduction

   Tools created in python to manipulate and analyse large datasets. 
   * Make sure you have python 3.11+ installed.
   * To start using clone the repo and run **Update.bat** file.
   * To run any of the script open cmd or powershell and run "`python [name of the script].py`"
   * To run parallel tasks open a bash terminal instead of cmd or powershell and run "`python [name of the script 1].py & python [name of the script 2].py  &`", add as many processes as you want to run, just make sure to add the last "`&`".

## Split CSV

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

      Splits to multiple folders based on the column name in the reference file. eg:- if the reference file looks like this, and the input file and selected column is the same as the example in **Column Values**, then the output will be: "output_dir/**spanishNames**/input1.csv", "output_dir/**englishNames**/input1.csv"

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
                     
## Split To Multiple Columns

   Split to Multiple Columns allows you to split one column based on the type of the value to multiple columns. The types of split possible are:

   * #### Name type value :
      
       Converts values like `{name : "John H. Doe"}` to 3 columns, `{firstName:"John", middleName: "H.", lastName:"Doe"}`

   * #### Address type value : 
   
      Converts values like `{address : "Boron CA US 93516"}` to 4 columns, `{city : "Boron", state : "CA", country : "US", zip : "93516"}`

## Join Multiple CSV

   Join multiple CSV files based on the column provided, Before running this, make sure you have the files you want to join to (***left***) in one folder, and the files you want to join with (***right***) in another folder.
   The ***left*** will be kept as is and only the new columns from the ***right*** will be added to it. You can have multiple ***left***  and ***right*** files, with completely different column headers.

   eg:- If your left and right file looks like this,

            Inputs:
               left1.csv  
               | name  | address | email           |
               | ----- | ------- | --------------- |
               | dave  | 1234    | dave@gmail.com  |
               | john  | 1234    | john@gmail.com  |
               | peter | 1234    | peter@gmail.com |
               | diane | 1234    | diane@gmail.com |
               
               ____________________________________________
               
               right1.csv
               | phoneNumber | e-mail          |
               | ----------- | --------------- |
               | 5684        | dave@gmail.com* |
               | 8654        | baz@gmail.com   |
               | 8654        | bar@gmail.com   |
               | 8654        | bo@gmail.com    |
               | 8654        | baz@gmail.com   |

               right2.csv
               | Fax   | Email           |
               | ----- | --------------- |
               | 8975  | dave@gmail.com* |
               | 87655 | john@gmail.com* |
               | 5486  | 545             |
               | 4851  | fum@gmail.com   |

            Outputs:
               output_dir/left1.csv
               | name  | address | email           | Fax     | phoneNumber |
               | ----- | ------- | --------------- | ------- | ----------- |
               | dave  | 1234    | dave@gmail.com  | 8975.0  | 5684        |
               | john  | 1234    | john@gmail.com  | 87655.0 |             |
               | peter | 1234    | peter@gmail.com |         |             |
               | diane | 1234    | diane@gmail.com |         |             |

               
## Filter ACN

   Filter out the age country or names from a csv file ***WIP***

## Remove Columns

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

## Remove Null Values

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


## Reorder Columns

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

## Change Encoding

   Change the encoding of the files in the input directory to utf-8, any illegal character will be replaced with a "`?`". 