package cs435.termProject.debtPerInstitution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Alec on 4/28/2017.
 * Driver for finding average debt of graduates by institution
 */
public class DebtDriver {

	public static int run(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);

		Job job = Job.getInstance(configuration);
		job.setJarByClass(DebtDriver.class);
		job.setMapperClass(DebtMapper.class);
		job.setReducerClass(DebtReducer.class);
		//job.setSortComparatorClass(DoubleComparator.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		if (fileSystem.exists(output)) {
			fileSystem.delete(output, true);
		}

		return (job.waitForCompletion(true) ? 1 : 0);
	}

}
