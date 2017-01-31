*******************************************************************
				README
*******************************************************************

MAKE FILE
*******************************************************************
makefile, src and pom.xml must be present in the same directory.


Make file has two tasks:
a) run : builds the project jar using maven
The jar would be created in a target sub-directory,

b) local: executes the jar
The jar expects the input ".csv" file in the same directory as make file.

*******************************************************************
SOURCE DIRECTORY STRUCTURE
*******************************************************************
The source code directory includes the following Java source files:

PACKAGE: com.mapreduce

MODEL: 
Station
A model that stores running TMAX and record count for station

LOADER/MAIN CLASS:
LOADER
This parses the data from input file, and calls the starter functions for all the implementations.

SEQUENTIAL:
Sequential
SequentialFib (with Fibonacci delay)

NO-LOCK:
NoLock
NoLockFib (with Fibonacci delay) 


COARSE-LOCK:
CoarseLock
CoarseLock (with Fibonacci delay)

FINE-LOCK:
FineLock
FineLock (with Fibonacci delay) 

NO-SHARING:
NoSharing
NoSharingFib (with Fibonacci delay)

**************************************************************************