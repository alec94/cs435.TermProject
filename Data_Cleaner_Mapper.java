package pkg;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Data_Cleaner_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	TreeMap<String, Integer> selective_columns = new TreeMap<String, Integer>();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Path path = new Path(context.getConfiguration().get("Column_Selector"));
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		
		String line;
		while((line = br.readLine()) != null) {
			String[] words = line.split("\\|+");
			
			if(words.length >= 7) {
				String[] keep = words[1].toLowerCase().trim().replaceAll("[^0-9a-z]", " ").split("\\s+");
				if(keep[0].equals("definitely"))
					selective_columns.put(words[6], 0);
			}
		}
		
		
		
		path = new Path(context.getConfiguration().get("Column_Selector"));
		fs = FileSystem.get(new Configuration());
		br = new BufferedReader(new InputStreamReader(fs.open(path)));
		
		line = br.readLine();
		String[] columns = line.split(",");
		
		for(int i = 0; i < columns.length; i++) {
			if(selective_columns.getOrDefault(columns[i], -1) != -1) {
				selective_columns.put(columns[i], i);
			}
		}
		
		br.close();
	}
	
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
		String[] columns = line.toString().trim().split(",");
		
		int index = 377;
		if (columns[index] != null && !columns[index].isEmpty())
			index = 378;
		
		double rti = Double.valueOf(columns[index]) / Double.valueOf(columns[1688]);
		
		context.write(new Text("rti_by_institute"), new Text(columns[selective_columns.get("INSTNM")] + " " + String.valueOf(rti)));
	}
}