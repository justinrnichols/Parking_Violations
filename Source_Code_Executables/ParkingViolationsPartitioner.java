package parking_violations;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class ParkingViolationsPartitioner<K2, V2> extends Partitioner<Text, Text> implements Configurable {

	/*
	 * Global Variable Declarations:
	 * configuration: used to configure the partitioner to partition by season.
	 * seasons: stores the seasons starting at Summer=0.
	*/
	private Configuration configuration;
	HashMap<String, Integer> seasons = new HashMap<String, Integer>();

	@Override
	public void setConf(Configuration configuration) {
		this.configuration = configuration;
		// Add each (columnName, columnIndex) key-value to the HashMap.
		seasons.put("Summer", 0);
		seasons.put("Fall", 1);
		seasons.put("Winter", 2);
		seasons.put("Spring", 3);
	}
	
	@Override
	public Configuration getConf() {
		return configuration;
	}

	public int getPartition(Text key, Text value, int numReduceTasks) {
		// Split the value by commas and obtain the season name.
		String season = value.toString().split(",")[0];
		// Partition the intermediate data by season.
		return (int) (seasons.get(season));
	}
  
}
