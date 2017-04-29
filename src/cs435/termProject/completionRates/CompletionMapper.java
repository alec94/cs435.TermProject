package cs435.termProject.completionRates;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Alec on 4/28/2017.
 * gets completion percentages by race per year
 */
public class CompletionMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0) { // throw out first line
			return;
		} else {
			String[] path = ((FileSplit) context.getInputSplit()).getPath().getName().split("/");
			String year = path[path.length - 1].substring(6, 13).replace("_", "-");

			String[] split = value.toString().split(",");

			String white = split[396];
			String black = split[397];
			String hispanic = split[398];
			String asian = split[399];
			String american = split[400]; // american indian and american alaskan
			String islander = split[401]; // native hawaiian and pacific islander
			String mixed = split[402]; // two or more races
			String alien = split[403]; // non resident alien
			String unknown = split[404];
			String all = split[386]; // rate for all students

			String result = white + ":" + black + ":" + hispanic + ":" + asian + ":" + american + ":" + islander + ":" + mixed + ":" + alien + ":" + unknown + ":" + all;

			context.write(new Text(year), new Text(result));
		}
	}
}
