package cs435.termProject.degreeTypeRTI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Alec on 4/29/2017.
 * calculates averages rti for a year
 */
public class RTIReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] costTotal = {0, 0, 0, 0, 0};
		int[] costCount = {0, 0, 0, 0, 0};
		double[] earningsTotal = {0, 0, 0, 0, 0};
		int[] earningsCount = {0, 0, 0, 0, 0};
		double[] avgEarnings = new double[5];
		double[] avgCost = new double[5];
		String[] rti = new String[5]; // string so NULL can be included

		for (Text value : values) {
			String[] split = value.toString().split(":");
			int degree = Integer.parseInt(split[0]);

			costTotal[degree] += Integer.parseInt(split[1]);
			costCount[degree]++;

			earningsTotal[degree] += Integer.parseInt(split[2]);
			earningsCount[degree]++;
		}

		//calculate average cost and earnings
		for (int i = 0; i < 5; i++) {
			if (costCount[i] != 0) {// no data for degree type
				avgCost[i] = costTotal[i] / costCount[i];
			} else {
				avgCost[i] = -9999;
			}

			if (earningsCount[i] != 0) {
				avgEarnings[i] = earningsTotal[i] / earningsCount[i];
			} else {
				avgEarnings[i] = -9999;
			}
		}

		// calculate rti for each degree type

		for (int i = 0; i < 5; i++) {
			if (avgCost[i] == -9999 || avgEarnings[i] == -9999) { // no data for degree type
				rti[i] = "NULL";
			} else {
				rti[i] = avgCost[i] / avgEarnings[i] + ""; // save as string
			}
		}

		StringBuilder result = new StringBuilder();

		for (int i = 0; i < 5; i++) {
			result.append(getDegreeType(i)).append(": ").append(rti[i]).append("\n\t");
		}

		context.write(key, new Text(result.toString()));

	}

	private String getDegreeType(int index) {
		String degreeName = "";

		switch (index) {
			case 0:
				degreeName = "Not classified";
				break;
			case 1:
				degreeName = "Certificate";
				break;
			case 2:
				degreeName = "Associate's";
				break;
			case 3:
				degreeName = "Bachelor's";
				break;
			case 4:
				degreeName = "Graduate's";
				break;
		}

		return degreeName;
	}
}
