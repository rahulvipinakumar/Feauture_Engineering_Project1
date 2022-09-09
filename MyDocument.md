Dataset analysis:
-----------------
28 columns are present.
4 Numerical Columns, 20 text columns and 4 DateTime columns
Posting Type and Salary Frequency are main categorical columns.
14 columns are having missing values

Below 8 columns are having NULL values less than 10% of total records
To Apply                         0.000339
Job Category                     0.000679
Posting Date                     0.001358
Residency Requirement            0.001358
Process Date                     0.001358
Posting Updated                  0.001358
Minimum Qual Requirements        0.006789
Full-Time/Part-Time indicator    0.066191

Below 3 columns are having NULL values more than valid values.
Work Location 1                  0.539036
Hours/Shift                      0.699932
Post Until                       0.704345

The column 'Recruitment Contact' doesnt have any valid values.


Problems faced:

Issue: NULL values in Full-Time/Part-Time indicator column.
Solution: Fill the NULLs with Mode value.
Blocker: While calculating the Mode of Full-Time/Part-Time indicator column based on Agency, we got many modes with NULL values.
Remedy: Finally we end up in using Salary Frequency for determining the mode value for Full-Time/Part-Time indicator column.

Issue: NULL values in Posting Date column.
Solution: Fill the NULLs with Mode value.
Remedy: We used Agency and "Posting Type" columns for determining the mode value for Posting Date column.

By,
Rahul Vipinakumar
