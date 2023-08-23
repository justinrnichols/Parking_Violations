package parking_violations;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ParkingViolationsMapper extends Mapper<LongWritable, Text, Text, Text> {

	/*
	 * Global Variable Declarations:
	 * columns: stores (columnName, columnIndex) to use for reference.
	 * month_season: stores (monthIndex, seasonName) to use for reference.
	 * taskType: stores the task for the program to map-reduce by, default is vehicleMake.
	 */
	HashMap<String, Integer> columns = new HashMap<>();
	HashMap<Integer, String> month_season = new HashMap<>();
	private String taskType = "vehicleMake";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Split the value by commas (regexp needed for csv format).
		String[] line = value.toString().split("(?:\"[^\"]*\"|)\\s*(,\\s*|$)");
		// Filter out lines that have less than 35 columns.
		if (line.length >= 35) {
			// Store the relevant column data.
			String vehicleBodyType = line[columns.get("Vehicle Body Type")];
			String vehicleMake = line[columns.get("Vehicle Make")];
			String vehicleYear = line[columns.get("Vehicle Year")];
			String vehicleColor = line[columns.get("Vehicle Color")];
			String registrationState = line[columns.get("Registration State")];
			String plateType  = line[columns.get("Plate Type")];
			String issueDate = line[columns.get("Issue Date")];		
			String violationCode = line[columns.get("Violation Code")];
			
			/* 
			 * Assign the type to the specific taskType column data.
			 * I.e., if taskType="plateType", get the data from the plateType column.
			 * Provides ability to filter out all lines that are missing data at the specific 
			 * task column, since it is required and will be the map-reduce key.
			 */
			String type = "";
	    	switch (taskType) {
	    		case "registrationState":
	    			type = registrationState;
	    			break;
	    		case "plateType":
	    			type = plateType;
	    			break;
	    		case "vehicleBodyType":
	    			type = vehicleBodyType;
	    			break;
	    		case "vehicleMake":
	    			type = vehicleMake;
	    			break;
	    		case "vehicleColor":
	    			type = vehicleColor;
	    			break;
	    		case "vehicleYear":
	    			type = vehicleYear;
	    			break;
	    	}
			
	    	/*
	    	 * Filter out the first line (legend).
	    	 * Filter out any line that is missing data at the taskType, issueDate, or violationCode columns.
	    	 */
			if (!line[0].equals("Summons Number")) {
				if (type.isEmpty() || issueDate.isEmpty() || violationCode.isEmpty()) {;}
				else {
					/*
					 * Parse the date to verify its in a valid format.
					 * Filter out misformatted and invalid dates.
					 */
					Calendar cal = null;
					try {
						cal = Calendar.getInstance();
						SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy");
						cal.setTime(sdf.parse(issueDate));
					} 
					catch (ParseException e) {
						System.out.println(value.toString());
						e.printStackTrace();
					}
					
					/*
					 * Collect the month value to index what season its in.
					 * Concatenate the month value to the violationCode value as this will 
					 * be the map-reduce value.
					 */
			    	if (cal != null) {
			    		int month = cal.get(Calendar.MONTH);
						String violationInfo = month_season.get(month) + "," + violationCode;
						
						// Write out the task (taskType) data as the key and the violationInfo as the value.
						context.write(new Text(type), new Text(violationInfo));
			    	}
				}
			}
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		/*
		 * Variable Declarations:
		 * configuration: used to configure the mapper to use a ToolRunner argument.
		 * options: stores the valid typeTask values.
		*/
		Configuration conf = context.getConfiguration();
		ArrayList<String> options = new ArrayList<>(Arrays.asList("registrationState", "plateType", "vehicleBodyType", "vehicleMake", "vehicleColor", "vehicleYear"));
		
		// Add each (columnName, columnIndex) key-value to the HashMap.
		columns.put("Registration State", 2);
		columns.put("Plate Type", 3);
		columns.put("Issue Date", 4);
		columns.put("Violation Code", 5);
		columns.put("Vehicle Body Type", 6);
		columns.put("Vehicle Make", 7);
		columns.put("Vehicle Color", 33);
		columns.put("Vehicle Year", 35);
		
		// Add each (monthIndex, seasonName) key-value to the HashMap.
		month_season.put(0, "Winter");
		month_season.put(1, "Winter");
		month_season.put(2, "Spring");
		month_season.put(3, "Spring");
		month_season.put(4, "Spring");
		month_season.put(5, "Summer");
		month_season.put(6, "Summer");
		month_season.put(7, "Summer");
		month_season.put(8, "Fall");
		month_season.put(9, "Fall");
		month_season.put(10, "Fall");
		month_season.put(11, "Winter");

		/*
		 * Store the user given taskType value from ToolRunner.
		 * Default the taskType to vehicleMake, if the user does not define a taskType.
		 * If the user provides an invalid taskType, throw an error.
		 * Otherwise, assign the taskType to the user given value.
		 */
		taskType = conf.get("taskType");
		if (taskType == null)
			taskType = conf.getTrimmed("taskType", "vehicleMake");
		if (!options.contains(taskType))
			throw new IOException("Invalid option");
		else
			taskType = conf.getTrimmed("taskType", taskType);
	}
	
}