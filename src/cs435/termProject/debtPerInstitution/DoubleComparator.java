package cs435.termProject.debtPerInstitution;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;

import java.nio.ByteBuffer;

/**
 * Created by Alec on 4/28/2017.
 * used to sort output by value instead of key
 */
public class DoubleComparator extends WritableComparator {
	public DoubleComparator() {
		super(DoubleWritable.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1,
					   byte[] b2, int s2, int l2) {

		Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
		Double v2 = ByteBuffer.wrap(b2, s2, l2).getDouble();

		return v1.compareTo(v2) * (-1);
	}


}
