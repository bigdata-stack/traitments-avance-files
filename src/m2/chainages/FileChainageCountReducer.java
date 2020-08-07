package m2.chainages;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileChainageCountReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context sortie)
			throws IOException, InterruptedException {

		Iterator<IntWritable> it = values.iterator();
		int currentCount = 0;
		while (it.hasNext()){
			it.next();
			currentCount++;
		}
		sortie.write(new Text(key.toString()), new IntWritable(currentCount));
	}
}