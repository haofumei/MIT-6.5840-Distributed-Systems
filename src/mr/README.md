# Lab 1 Report

## Idea

The idea of Map Reduce model has one coordinator and workers

Coordinator:

Running until all works done:

1. assign works
   1. if there are unassigned map tasks, assign mtask
   2. if there are unassigned reduce tasks and map tasks are done, assign rtask
   3. if all the works have been done, ask workers to exit
   4. else ask workers to wait
   5. create a thread to refresh the assigned work
2. checkout works done
3. refresh assigned work after a timeout

Workers:

Running until all works done:

1. ask coordinator for work, and make decision based on the response
   1. if it is a map or reduce task, do it
   2. if it is a wait, wait some times
   3. if it is a exit, exit
2. submit the finished work

## Details:

1. How to avoid created intermediate files conflit?
   * Workers create a temporary file first, and then rename the tmp file to corresponding intermediate file. And we will name the intermediate file in this way, for example, the worker who are in charge of the 1st map task, and number of reduce tasks is 10, it will create mr-1-0, mr-1-1, mr-1-2, ... mr-1-9 files.
   * os.Rename() will replace the content of old file to new file, and the new file will keep its original file descriptor. So we don't need to worry about some situations like, there are some slow workers working on map task after the reduce tasks have been assigned. Even though they modify the intermediate files, the reduce works still can access the same contents.
2. How should coordinator deal with late submitted task?
   * Just ignore them, like I said above, even though the late workers modify the intermediate, the reduce workers still can access the same content.

## Some thoughts:

This lab only considers 8 map tasks and 10 reduce tasks, but there may be hundreds or thousands of tasks in reality. Therefore, a loop that searching for tasks to assign in the mutex may slow the process of coordinator. It had better storing the unassigned tasks within a channel, so the task could be withdrawn immediately when it was needed. But I did not implement it n this case.

## References:

1. Jeffrey Dean and Sanjay Ghemawat (2004). "MapReduce: Simplified Data Processing on Large Clusters." Proceedings of the 6th Symposium on Operating System Design and Implementation (OSDI), San Francisco, CA, USA. Available online: [https://www.usenix.org/legacy/event/osdi04/tech/full_papers/dean/dean.pdf](https://www.usenix.org/legacy/event/osdi04/tech/full_papers/dean/dean.pdf).
