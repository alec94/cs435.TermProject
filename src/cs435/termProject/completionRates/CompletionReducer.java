package cs435.termProject.completionRates;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Alec on 4/28/2017.
 * calculates average completion rates for that year
 */
public class CompletionReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


//		double whiteTotal = 0;
//		int whiteCount = 0;
//		double blackTotal = 0;
//		int blackCount = 0;
//		double hispanicTotal = 0;
//		int hispanicCount = 0;
//		double asainTotal = 0;
//		int asainCount = 0;
//		double americanTotal = 0;
//		int americanCount = 0;
//		double islanderTotal = 0;
//		int islanderCount = 0;
//		double mixedTotal = 0;
//		int mixedCount = 0;
//		double alienTotal = 0;
//		int alienCount = 0;
//		double unknownTotal = 0;
//		int unknownCount = 0;

		double[] total = {0, 0, 0, 0, 0, 0, 0, 0, 0};
		int[] count = {0, 0, 0, 0, 0, 0, 0, 0, 0};
		String[] avg = new String[9];


		for (Text value : values) {
			String[] rate = value.toString().split(":");

			for (int i = 0; i < total.length; i++) {
				if (!rate[i].equals("NULL")) {
					total[i] += Double.parseDouble(rate[i]);
					count[i]++;
				}
			}

		}

		for (int i = 0; i < total.length; i++) {
			if (count[i] == 0) {
				avg[i] = "NULL"; // no data for that year
			} else {
				avg[i] = (total[i] / count[i]) * 100  + "%"; //save as string
			}
		}

		String result = "White: " + avg[0] + "\n\tBlack: " + avg[1] + "\n\tHispanic: " + avg[2];
		result += "\n\tAsian: " + avg[3] + "\n\tAmerican Indian/Alaska Native: " + avg[4] + "\n\tTwo-or-more-races: " + avg[5];
		result += "\n\tNon-resident alien: " + avg[6] + "\n\tUnknown race: " + avg[7] + "\n\tAll: " + avg[8];

		context.write(key, new Text(result));


	}
}
