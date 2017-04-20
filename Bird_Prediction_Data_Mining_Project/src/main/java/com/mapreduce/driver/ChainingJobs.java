package com.mapreduce.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;

import com.mapreduce.prediction.Prediction;
import com.mapreduce.training_and_validation.Train;
import com.mapreduce.training_and_validation.Validate;

public class ChainingJobs {

	public static void main(String args[]) throws Exception{
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);

		String labledData = args[0];
		String unlabledData = args[1];
		String outputDirectory = args[2];
		String finalOutputDirectory = args[3];

		String cleanOutput = outputDirectory+"/cleaning-output";
		String trainOutput = outputDirectory+"/training-output";
		String validationOutput =outputDirectory+ "/validation-output";

		// Cleaning 
		Cleansing.cleanData(labledData, cleanOutput);

		// Training
		Train.runLoader(cleanOutput+"/TRAIN-r-00000",trainOutput);

		// Validating
		Validate.test(cleanOutput+"/TEST-r-00000",validationOutput,trainOutput);

		// Predicting
		Prediction.runPrediction(unlabledData,finalOutputDirectory,trainOutput);
	}
}
