package pkg;


import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Data_Cleaner_Reducer extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		TreeMap<Double, String> rti_by_ist_name = new TreeMap<Double, String>();
		
		for(Text val: values) {
			String[] name_rti = val.toString().trim().split("\\s+");
			
			rti_by_ist_name.put(Double.valueOf(name_rti[1]), name_rti[0]);
		}
		
		for(Map.Entry<Double, String> entry : rti_by_ist_name.descendingMap().entrySet()) {
			context.write(new Text(entry.getValue()), new Text(String.valueOf(entry.getKey())));
		}
	}
}