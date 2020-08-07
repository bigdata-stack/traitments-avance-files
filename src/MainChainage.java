
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainChainage {

	public static void main(String[] args) throws Exception {
		//main du job1 
		Job job1 = new Job();
		Configuration conf = job1.getConfiguration();
		// get Param file 1
		conf.set("nomFichier1", args[2]);
		conf.set("colonneID1", args[3]);
		conf.set("listeDesColonnesProjetFile1", args[4]);
		//get Param  file 2
		conf.set("nomFichier2", args[5]);
		conf.set("colonneID2", args[6]);
		conf.set("listeDesColonnesProjetFile2", args[7]);
		
		
		conf.set("ordre", args[8]);
		conf.set("separateur", args[9]);
		
		job1.setJobName("The First Job1 in chainage  ");
		job1.setJarByClass(MainChainage.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(m2.stats.StateMapper.class);
		job1.setReducerClass(m2.stats.StateReducer.class);

		FileInputFormat.setInputPaths(job1, args[0]);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		FileSystem fs = FileSystem.get(job1.getConfiguration());
		fs.delete(new Path(args[1]));

		job1.waitForCompletion(true);
		//main du second Job 
		Job job2 = new Job();
		
		job2.setJobName("The First Job2 in chainage");
		job2.getConfiguration().set("separateur",args[9]);
		job2.setJarByClass(MainChainage.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		job2.setMapperClass(m2.stats.StateCountMapper.class);
		job2.setReducerClass(m2.stats.StateCountReducer.class);

		FileInputFormat.setInputPaths(job2, args[1]);
		FileOutputFormat.setOutputPath(job2, new Path(args[10]));

		fs = FileSystem.get(job2.getConfiguration());
		fs.delete(new Path(args[10]));
		job2.waitForCompletion(true);
	}

}
