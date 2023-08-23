package parking_violations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

public class ParkingViolationsDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// Get the start time
		final long startTime = System.currentTimeMillis();
		/*
		 * Allow configuration changes through the use of the Tool interface.
		 */
		int exitCode = ToolRunner.run(new Configuration(), new ParkingViolationsDriver(), args);
		// Get the end time
		final long endTime = System.currentTimeMillis();
		// Calculate total running time of program.
		System.out.println("Total execution time: " + (endTime - startTime));
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
	    /*
	     * Validate that two arguments were passed from the command line.
	     */
		if (args.length != 2) {
			System.out.printf("Usage: ParkingViolationsDriver <input dir> <output dir>\n");
			System.exit(-1);
		}

	    /*
	     * Print the MapReduce log on local console.
	     * Instantiate a Job object for the job's configuration. 
	     */
	    // PropertyConfigurator.configure("log4j.properties");
	    Configuration conf = new Configuration();
	    // Job job = Job.getInstance(getConf(), "Parking Violations");
	    Job job = Job.getInstance(conf, "Parking Violations");
	    
	    /*
	     * The jar file that contains the driver, mapper, and reducer.
	     */
	    job.setJarByClass(ParkingViolationsDriver.class);
	
	    /*
	     * Set the input and output paths for the job.
	     */
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
	    /*
	     * Link the Mapper, Reducer, and Partitioner class to the driver.
	     */
	    job.setMapperClass(ParkingViolationsMapper.class);
	    job.setReducerClass(ParkingViolationsReducer.class);
	    job.setPartitionerClass(ParkingViolationsPartitioner.class);
	    
	    /*
	     * Set the number of reduce tasks to 4 (one for each season).
	     */
	    job.setNumReduceTasks(4);

	    /*
	     * Set the input and output key and value types for the Mapper.
	     */
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    /*
	     * Set the input and output key and value types for the Reducer.
	     */
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
  
}