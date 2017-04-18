In this work, I used the Hadoop's MapReduce framework to construct Equal-Join, one of the database operation.
- Driver: I initialized the job with "EquiJoin by Mapreduce”; moreover, I defined the type of KEYIN, VALIN, KEYOUT,VALOUT as Text.

- Mapper:
    - KEY: the number of join column
    - VAL: the complete line from input file

- Reducer:
    - I use two arraylist to category these two table independently and then I linked the two corresponding lines into one line as the key of output.
