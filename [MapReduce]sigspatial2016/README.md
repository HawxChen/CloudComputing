# ACM Sigspatial Cup 2016
#### Distributed Computation of MapReduce on geospatial data.
------

This work implements the solution for the [ACM SIGSPATIAL Cup 2016](http://sigspatial2016.sigspatial.org/giscup2016/problem) to justify the top fifty hotspots for the cabs in NY City under the following constraints.
- Time step size is 1 day.
- Input data is 2015 January Yellow Taxi and its size should be around 2GB.
- The unit size in both latitude and longitude degrees is 0.01.
- This problem cares about pick-up location instead of drop-down location.
- The attribute we care considered in cell is the number of trips instead of the number of passengers.

Implementation Requirement
- The core metric of calculation is Getis-Ord statistic and implemented through JAVA, Apache Spark, and Hadoop.
- The platform for evaluation is based 16 servers constructed on AWS.
- The tools I devloped for cluster management are placed in [Cluster Management](https://github.com/HawxChen/CloudComputing/tree/master/%5BSysAdmin%5DClusters)
