package cs435.termProject.debtPerInstitution;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Alec on 4/28/2017.
 */
public class DebtMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0) { // first line of file, throw it out
			return;
		} else {
			String[] split = value.toString().split(",");
			String debt = split[1495];

			if (!debt.equals("NULL") && !debt.equals("PrivacySuppressed")) {
				try {
					context.write(new Text(split[3]), new DoubleWritable(Double.parseDouble(debt)));
				} catch (NumberFormatException e) {
					System.out.println("Cannot parse double '" + debt + "'");
				}
			}
		}
	}
}
