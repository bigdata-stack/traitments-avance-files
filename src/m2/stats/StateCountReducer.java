package m2.stats;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StateCountReducer extends Reducer<IntWritable, IntWritable, Text, DoubleWritable> {

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context sortie)
			throws IOException, InterruptedException {

		Iterator<IntWritable> it = values.iterator();
		Configuration conf = sortie.getConfiguration();
		String separateur=conf.get("separateur");
		String[] keys=key.toString().split(separateur);
		Double densite=Double.valueOf(keys[2]);
		int currentCount = 0;
		while (it.hasNext()){
			it.next();
			currentCount++;
		}
		if(currentCount!=0)
			sortie.write(new Text(key.toString().replace(separateur+keys[keys.length-1], "")), new DoubleWritable(currentCount/densite)); 

	}
}