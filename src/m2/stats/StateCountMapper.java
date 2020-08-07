package m2.stats;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StateCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {
		
		try {
			Configuration conf = sortie.getConfiguration();
			String separateur=conf.get("separateur");
			String[] line = valeur.toString().split("\t");	
			String keys=valeur.toString().split("\t")[0];	
			line=line[1].split(separateur);
			if(!line[0].isEmpty() && !line[1].isEmpty()) {
				sortie.write(new Text(keys.toString()+separateur+line[0]+separateur+line[1]), new IntWritable(1));
			}
			
		} catch (Exception ee) {
			throw new RuntimeException(ee.getMessage());
		}
	}
}


