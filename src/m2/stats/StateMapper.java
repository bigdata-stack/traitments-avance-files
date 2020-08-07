package m2.stats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StateMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {
		try {
			String nomFichier = ((FileSplit) sortie.getInputSplit()).getPath().getName();
			Configuration conf = sortie.getConfiguration();
			String getNameFile1 = conf.get("nomFichier1");
			String getNameFile2 = conf.get("nomFichier2");
			int getColonneID1 = Integer.valueOf(conf.get("colonneID1"));
			int getColonneID2 = Integer.valueOf(conf.get("colonneID2"));
			String[] getListColonParam1 = conf.get("listeDesColonnesProjetFile1").split(",");
			String[] getListColonParam2 = conf.get("listeDesColonnesProjetFile2").split(",");
			String separateur = conf.get("separateur");
			String keys = "";
			String valeurs = "";
			if (cle.get() != 0) {
				if (nomFichier.equalsIgnoreCase(getNameFile1)) {

					String[] line = valeur.toString().split(";");

					for (String s : getListColonParam1) {
						if (!line[Integer.valueOf(s)].isEmpty())
							valeurs += separateur + line[Integer.valueOf(s)].replaceAll("\"", "").trim();
					}

					sortie.write(new Text(line[Integer.valueOf(getColonneID1)].replaceAll("\"", "")),
							new Text(valeurs));

				} else if (nomFichier.equalsIgnoreCase(getNameFile2)) {
					String[] line = valeur.toString().split(",");

					for (String s : getListColonParam2) {
						if (!line[Integer.valueOf(s)].isEmpty())
							valeurs += separateur + line[Integer.valueOf(s)].replaceAll("\"", "").trim();
					}
					sortie.write(new Text(line[Integer.valueOf(getColonneID2)].replaceAll("\"", "")),
							new Text(valeurs));
				}
			}

		} catch (Exception ee) {
			throw new RuntimeException(ee.getMessage());
		}
	}
}
