package m2.chainages;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileChainageCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {
		
		try {
			String[] line = valeur.toString().split("\t");	
			sortie.write(new IntWritable(Integer.valueOf(line[0])), new IntWritable(1));
			
		} catch (Exception ee) {
			throw new RuntimeException(ee.getMessage());
		}
	}
}


