package cs435.termProject;

import cs435.termProject.completionRates.CompletionDriver;
import cs435.termProject.debtPerInstitution.DebtDriver;
import cs435.termProject.degreeTypeRTI.RTIDriver;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Alec on 4/24/2017.
 * main driver for map reduce algorithm
 */
public class Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length < 2) {
			System.out.println("ERROR: Wrong number of arguments\nUSAGE: <input file> <output folder>");
			System.exit(-1);
		}

		Path input = new Path(args[0]);

		DebtDriver.run(new Path(args[0]), new Path(args[1] + "/debt"));
		CompletionDriver.run(input, new Path(args[1] + "/comp"));
		RTIDriver.run(input, new Path(args[1] + "/degreeRTI"));

	}
}


