# CS3223 Project (Database Systems Implementation)
### _AY2020/2021 Semester 2, School of Computing, National University of Singapore_
## Team members :
* [Erin May Gunawan](https://github.com/erinmayg/)
* [Florencia Martina](https://github.com/florenciamartina/)
* [Quek Wei Ping](https://github.com/qweiping31415)

## Project Summary

This is an implementation of a simple SPJ (Select-Project-Join) query engine to illustrate query processing in a modern relational database management system. 
The link to our full report can be found [here](https://docs.google.com/document/d/14WCwnRypLCUQyu2RkJQTVtppkGr6nzu2mxYdzZY9Jp4/edit#).

## Implemented Features

#### 1. Block Nested Loop Join (See [BlockNestedLoopJoin.java](https://github.com/florenciamartina/CS3223-project/blob/master/src/qp/operators/BlockNestedLoopJoin.java))
#### 2. Sort Merge Join based on Sort (See [SortMergeJoin.java](https://github.com/florenciamartina/CS3223-project/blob/master/src/qp/operators/SortMergeJoin.java))
#### 3. Distinct based on Sort (See [Distinct.java](https://github.com/florenciamartina/CS3223-project/blob/master/src/qp/operators/Distinct.java))
#### 4. Groupby based on Sort (See [GroupBy.java](https://github.com/florenciamartina/CS3223-project/blob/master/src/qp/operators/GroupBy.java))
#### 5. Orderby based on Sort (See [OrderBy.java](https://github.com/florenciamartina/CS3223-project/blob/master/src/qp/operators/OrderBy.java))   
#### 6. Identified and fixed the following bugs/limitations in the SPJ engine given:
* If the table's tuple size is bigger than the buffer size, SPJ goes to infinity loop.
* If the query does not involve join, the SPJ does not require the user to input the number of buffers.
* If a join query involves more than one join condition on two same tables, the number of tuples is higher than expected.

_Click [here](https://www.comp.nus.edu.sg/~tankl/cs3223/project.html) for more details regarding the project requirement._ 
