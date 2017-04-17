# ACM Sigspatial Cup 2016
#### Distributed Computation of MapReduce on Geospatial Data.
------

This work implements the solution for the [ACM SIGSPATIAL Cup 2016](http://sigspatial2016.sigspatial.org/giscup2016/problem) to justify the top fifty hotspots for the cabs in NY City under the following constraints.
- Time step size is 1 day.
- Input data is 2015 January Yellow Taxi and its size should be around 2GB.
- The unit size in both latitude and longitude degrees is 0.01.
- This problem cares about pick-up location instead of drop-down location.
- The attribute we care considered in cell is the number of trips instead of the number of passengers.

Enviroment
- 16 inexpensive commodity servers on AWS
  - 1 server in t2.medium level is reponbisble for the role: master.
  - 15 servers in t2.micro level are slaves.
- The tools I devloped for cluster management are placed in [Cluster Management](https://github.com/HawxChen/CloudComputing/tree/master/%5BSysAdmin%5DClusters).
- The core metric of calculation is Getis-Ord statistic.
- JAVA, Apache Spark, and Hadoop.

Result
- Earnee all points through completely matched profiled result for hotspots.
- Analyzed 12 million records in only 1 minute.

The following figure demonstrates resource usages for MapReduce computation between each server.
![screen shot 2017-04-17 at 09 37 43](https://cloud.githubusercontent.com/assets/1461806/25098904/1d63911e-235f-11e7-8500-8d1ab25579e3.png)

Other Team members: Sung, Jing, YD.
