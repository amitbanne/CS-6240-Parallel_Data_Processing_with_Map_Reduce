package com.mapreduce.loader;

import java.util.List;

import com.mapreduce.coarse_lock.CoarseLock;
import com.mapreduce.coarse_lock.CoarseLockFib;
import com.mapreduce.fine_lock.FineLock;
import com.mapreduce.fine_lock.FineLockFib;
import com.mapreduce.no_lock.NoLock;
import com.mapreduce.no_lock.NoLockFib;
import com.mapreduce.no_sharing.NoSharing;
import com.mapreduce.no_sharing.NoSharingFib;
import com.mapreduce.sequential.Sequential;
import com.mapreduce.sequential.SequentialFib;
import com.mapreduce.util.Util;

// Main class that initiates all the model executions
public class Loader {
	// initialize iteration count for each model
	private static final Integer ITERATION_COUNT = 10;

	public static void main(String[] args) {
		// read the input file name from command line
		String fileName = args[0];
		// parse the input data from the file
		//(only contains lines of text that are TMAX records)
		List<String> unparsedData = Util.fileParser(fileName);

		// pass file data and iteration count to all the models 
		
		// SEQUENTIAL
		Sequential.sequentialStarter(unparsedData, ITERATION_COUNT);

		// SEQUENTIAL FIBONACCI
		SequentialFib.sequentialFibStarter(unparsedData, ITERATION_COUNT);

		// NO_LOCKS
		NoLock.noLockStarter(unparsedData, ITERATION_COUNT);

		// NO_LOCKS_FIBONACCI
		NoLockFib.noLockFibStarter(unparsedData, ITERATION_COUNT);

		// COARSE LOCK
		CoarseLock.coarseLockStarter(unparsedData, ITERATION_COUNT);

		// COARSE LOCK FIBONACCI
		CoarseLockFib.coarseLockFibStarter(unparsedData, ITERATION_COUNT);

		// FINE LOCK
		FineLock.fineLockStarter(unparsedData, ITERATION_COUNT);

		// FINE LOCK FIBONACCI
		FineLockFib.fineLockFibStarter(unparsedData, ITERATION_COUNT);

		// NO SHARING
		NoSharing.noSharingStarter(unparsedData, ITERATION_COUNT);

		// NO SHARING FIBONACCI
		NoSharingFib.noSharingFibStarter(unparsedData, ITERATION_COUNT);

	}

}
