package cs435.termProject.degreeTypeRTI;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Alec on 4/29/2017.
 * gets values from data set
 */
public class RTIMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() != 0) { // throw out first line
			String[] split = value.toString().split(",");
			int degree = Integer.parseInt(split[14]);

			String[] path = ((FileSplit) context.getInputSplit()).getPath().getName().split("/");
			String year = path[path.length - 1].substring(6, 13).replace("_", "-");

			String cost = split[376]; //Average cost of attendance (academic year institutions)
			String earnings = split[1687]; //Median earnings of students working and not enrolled 8 years after entry

			if (cost.equals("NULL") || cost.equals("PrivacySuppressed")) {
				cost = split[377]; //Average cost of attendance (program-year institutions)
			}

			if (cost.equals("NULL") || cost.equals("PrivacySuppressed")) {
				cost = split[316]; //Average net price for Title IV institutions (public institutions)
			}

			if (cost.equals("NULL") || cost.equals("PrivacySuppressed")) {
				cost = split[317]; //Average net price for Title IV institutions (private for-profit and nonprofit institutions)
			}

			if (earnings.equals("NULL") || earnings.equals("PrivacySuppressed")){
				earnings = split[1630]; //Median earnings of students working and not enrolled 10 years after entry
			}

			if (!(cost.equals("NULL") || cost.equals("PrivacySuppressed") || (earnings.equals("NULL") || earnings.equals("PrivacySuppressed")))) { // unusable data
				context.write(new Text(year), new Text(degree + ":" + cost + ":" + earnings));
			}else {
//				System.out.println("No data for " + year);
//				System.out.println(cost +" - " + earnings);
			}


		}
	}

}
