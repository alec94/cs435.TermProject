package cs435.termProject.debtPerInstitution;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Alec on 4/28/2017.
 * reducer for average dept
 */

public class DebtReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		double total = 0;
		double avg = 0;

		for (DoubleWritable value : values) {
			total += value.get();
			count++;
		}

		avg = total / count;

		context.write(key, new DoubleWritable(avg));
	}
}
