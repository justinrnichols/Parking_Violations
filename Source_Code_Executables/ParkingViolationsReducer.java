package parking_violations;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ParkingViolationsReducer extends Reducer<Text, Text, Text, Text> {
	
	/*
	 * Global Variable Declarations:
	 * violationInfo: stores (violationCode, violationAmount) to use for reference.
	 */
	HashMap<Integer, Integer> violationInfo;
	
	/*
	 * Helper function used to gather violation data from txt file
	 * and adding it to the violationInfo HashMap.
	 */
	private HashMap<Integer, Integer> violationInfo() {
		HashMap<Integer, Integer> violationInfo = new HashMap<>();
		// Define a file path.
		File file = new File("parking_violation_codes.txt");
		System.out.println("\n\n" + file.getAbsolutePath() + "\n\n");
		Scanner scanner = null;
		try {
			scanner = new Scanner(file);
		}
		catch(FileNotFoundException e) {
			e.printStackTrace();;
		}
		// Parse each line, split by tab.
		if (scanner != null) {
			scanner.nextLine(); // Used to ignore first line (legend)
			String line = "";
			int violationCode, violationAmount;
			// Store (violationCode, violationAmount) in violationInfo HashMap.
			while (scanner.hasNext()) {
				line = scanner.nextLine();
				violationCode = Integer.parseInt(line.split("\t")[0]);
				violationAmount = Integer.parseInt(line.split("\t")[2]);
				violationInfo.put(violationCode, violationAmount);
			}
		}
		//scanner.close();
		return violationInfo;
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/*
		 * Variable Declarations:
		 * count: stores the number of values for the current key.
		 * totalDollars: stores the total dollars accumulated for each key.
		*/
		long count = 0;
	    long totalDollars = 0;
	    
	    /*
	     * Iterate through all the values for each key.
	     * Get the violation number and find the amount for that violation.
	     * Add the violation amount to the current totalDollars and increase the count.
	     */
	    for (Text value: values) {
	    	int violation = Integer.parseInt(value.toString().split(",")[1]);
	    	if (violationInfo.get(violation) != null)
	    		totalDollars += violationInfo.get(violation);
	    	count++;
	    }
	    
	    /*
	     * Filter out keys that have less than 5 values.
	     * Calculate the average by dividing the totalDollars by the count.
	     */
	    if (count >= 5) {
	      double average = (double) Math.round((totalDollars / (double) count) * 100) / 100;
	      String output = count + ", " + totalDollars + ", " + average;
	      
	      // Write out the type (taskType) data as the key and the count, totalDollars, average as the value.
	      context.write(key, new Text(output));
	    }
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		// Write out a legend to the top of each output file.
		context.write(new Text("Type"), new Text("Count, Total Dollars, Average"));
		// Populate the violationInfo HashMap.
		violationInfo = violationInfo();
	}
	
}