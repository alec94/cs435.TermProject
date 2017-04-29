package cs435.termProject.completionRates;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Alec on 4/28/2017.
 * driver for finding completion rates by race per year
 */
public class CompletionDriver {

	public static int run(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);

		Job job = Job.getInstance(configuration);
		job.setJarByClass(CompletionDriver.class);
		job.setMapperClass(CompletionMapper.class);
		job.setReducerClass(CompletionReducer.class);
		//job.setSortComparatorClass(DoubleComparator.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		if (fileSystem.exists(output)) {
			fileSystem.delete(output, true);
		}

		return (job.waitForCompletion(true) ? 1 : 0);
	}
}
