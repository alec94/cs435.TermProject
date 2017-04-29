package cs435.termProject.degreeTypeRTI;

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
 * Created by Alec on 4/29/2017.
 * driver for finding RTI per year by degree type
 */
public class RTIDriver {
	public static int run(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);

		Job job = Job.getInstance(configuration);
		job.setJarByClass(RTIDriver.class);
		job.setMapperClass(RTIMapper.class);
		job.setReducerClass(RTIReducer.class);
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
