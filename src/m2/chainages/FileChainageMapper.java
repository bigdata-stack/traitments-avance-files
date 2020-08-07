package m2.chainages;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileChainageMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {		
		try {
			String nomFichier = ((FileSplit)sortie.getInputSplit()).getPath().getName();
			String[] line = valeur.toString().split(";");			
			Configuration conf = sortie.getConfiguration();
			String getNameFile1 = conf.get("nomFichier1");
			String getNameFile2 = conf.get("nomFichier2");
			int getColonneID1 = Integer.valueOf(conf.get("colonneID1"));
			int getColonneID2 = Integer.valueOf(conf.get("colonneID2"));
			String[] getListColonParam1 = conf.get("listeDesColonnesProjetFile1").split(",");
			String[] getListColonParam2 = conf.get("listeDesColonnesProjetFile2").split(",");
			String separateur=conf.get("separateur");
			String keys="";
			String valeurs="";
			if (cle.get()!=0){
				if (nomFichier.equalsIgnoreCase(getNameFile1)){
					
					if (line[getColonneID1]!=null ) {
						keys=line[getColonneID1];
						valeurs=nomFichier;
						for (String item : getListColonParam1) {
							if(!line[Integer.valueOf(item)].isEmpty()) {								
								valeurs+=separateur+ line[Integer.valueOf(item)];
							}
						}
						sortie.write(new IntWritable(Integer.valueOf(keys)), new Text(valeurs));	
					}
						
				} else if (nomFichier.equalsIgnoreCase(getNameFile2)){
					if (line[getColonneID2]!=null ) {
						keys=line[getColonneID2];
						valeurs=nomFichier;
						for (String item : getListColonParam2) {
							if(!line[Integer.valueOf(item)].isEmpty()) {								
								valeurs+=separateur+ line[Integer.valueOf(item)];
							}
						}
						sortie.write(new IntWritable(Integer.valueOf(keys)), new Text(valeurs));	
					}
				}
			}
			
		} catch (Exception ee) {
			throw new RuntimeException(ee.getMessage());
		}
	}
}


