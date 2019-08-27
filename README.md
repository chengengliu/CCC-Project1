# CCC Project 
## Purpose of the project
This project is a duo-project. </br>
For subject COMP90024 Cluster and Cloud computing.</br>
This project basically uses the Hight Performance Computing facilities of the University of Melbourne, called Spartan. </br>
To keep things in short, the project will do a data extraction from the source file 'bigTwitter.json', which is an 11GB big file. The bottleneck of the project is dealing with io operation, since it is impossible to use json load method(either it is infeasible on a local machine ie. memory overflow or it will be super slow on a HPC). </br>
There are two tasks as defined in the spec:</br>
1. count total twitts in each grid. </br>
2. count total hashtags in each grid. </br>

hashtag is defind in such pattern: "SPACE#STRINGSPACE"

## General implementation ideas
In this project, we used mpi4py, a library implementing MPI(Message Passing Interface). Using MPI is extremely helpful on multiple cores communication. Doing so will improve program efficiency if you write the code in a 'MPI' style </br>
In order to understand our code, we highly recommand you to read some notes on MPI beforehand and understand some knowledge aobout master-slave programming tech. Basically we will have one process call master and the rest of processes(cores) will be the 'slaves' of that master core. </br>
The master node will assign tasks to each slave node and collect the finished tasks from the slave cores. </br>
Notice that our main idea is that every core will read in the file but only deal with line number that they should deal with, by calculating "counter 'mod' process_number == rank". 
</br>
We pre-filter the data before stored the data into memory. </br>
There are 2.5 million of lines in the file but after filtering out noise data, there are only 0.4 million of lines remaining. </br>
However, do know that this is not the fastest way of doing MPI. 
Another way is to split the file into chunks and let different cores read in 1/8 of the total file. We thought about the method but it is bit time consuming using some file pointers thus we just gave up. Another way instead of json.loads is using regular expression but our professor discourages to do so. 
## More
The project receives 8.5/10, with a full mark of correctness and efficiency(the program finishes generally within 60 seconds), but deducted marks for code cleaness/documentation and report quality. 
