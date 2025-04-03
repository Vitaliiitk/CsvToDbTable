Console application that inserts data from a csv into a database's table, defines duplicates and writes them into a new csv file.

Set up. In your DBMS execute SQL commands from the folder ProjectCsvToSql/SqlQueries:

1) SQLQueryCreateMyCsvDb.sql;
2) SQLQueryCreateProcedureForDuplicatesHandling.sql.
Based on your DMBS, check the MyDbConnection connection string in appsetting.json, and substitute on your.

Your question: "Assume your program will be used on much larger data files. Describe in a few sentences what you would change if you knew it 
would be used for a 10GB CSV input file."

My suggestion is to try to implement parallel processing of data. For example, separate all data into chunks. One thread works on reading
from a csv, and another thread tries to write to a database. Same approach for reverse operation: one thread reads from a database table, 
and another writes to a new a csv file. I think this will speed up the process. At the same time, need to be attentive, and find a solution to synchronize threads.

## Deliverables
After execution of the application, I had 29889 rows left in the database and 111 duplicates found.
