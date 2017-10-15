package hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class FileToHbase {

	/* Mapper */																// !-On change le mapper
	public static class BoobleMapper extends Mapper<LongWritable, Text, Text, Put>{

		private Text wordkey = new Text();	 /* Equivalent String */
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {


			StringTokenizer itr = new StringTokenizer(value.toString()," ");
			String format = context.getConfiguration().get("hbase_conf");

			while (itr.hasMoreTokens()) { 
				String date = itr.nextToken();	//DATE
				//String month = date.split("_")[1];
				String heure = itr.nextToken(); //HEURE
				//String valHeure = heure.split("_")[0];
				//int valMinute = Integer.parseInt(heure.split("_")[1]);
				String IP = itr.nextToken(); // IP
				String mots = itr.nextToken();
				if(!mots.contains("+"))
					mots += "+";
				
				wordkey.set("TOTO");
				String[] t = format.split("_");
				Put obj = new Put(DigestUtils.md5(value.toString()));
				obj.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(t[0]), Bytes.toBytes(date));
				obj.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(t[1]), Bytes.toBytes(heure));
				obj.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(t[2]), Bytes.toBytes(IP));
				obj.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(t[3]), Bytes.toBytes(mots));
			
				
				context.write(wordkey, obj);
				
				/*
        	"put 'connections','MD5HASH','cf1:hour','VALUE'"
        	"put 'connections','MD5HASH','cf1:adress','VALUE'"
        	"put 'connections','MD5HASH','cf1:keywords','VALUE'"*/
			}

		}
	}

	public static class BoobleReducer 
	extends TableReducer<Text,Text,Put> {

		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {


			String t = key.toString();
			String month = t.split(",")[0];
			String heure = t.split(",")[1];
			String tranche = t.split(",")[2];
			HashMap<String,Integer> mostused = new HashMap<String, Integer>();
			String res = null;
			int max = 0;
			Text resultat;
			for(Text words : values) {

				String[] wordsplit = words.toString().split("\\+");


				for(String word : wordsplit) {
					if(mostused.containsKey(word)) {
						mostused.put(word, mostused.get(word)+1);
						if(mostused.get(word)>max) {
							res = word;
							max = mostused.get(word);
						}

					}else {
						mostused.put(word, 1);
					}
				}

			}

			/*
			if(Integer.parseInt(tranche)<30){
				resultat = new Text();
				resultat.set("MOIS :"+ month + "entre "+heure+":00 et "+heure+":30 "+res +" nbtot : " +max);
				context.write(key, resultat);
			}else {
				resultat = new Text();
				resultat.set("MOIS :"+ month +"entre "+heure+":30 et "+heure+":59 "+res +" nbtot : "+ max);
				context.write(key, resultat);
			}
			*/

		}
	}







	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: boobleHbase <Table> <in> <out hbase>");
			System.exit(2);
		}
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("table", args[0]);
		conf.set("hdfs_in", args[1]);
		conf.set("hbase_conf", args[2]);
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);



		Job job = Job.getInstance(conf, "booble count");
		job.setJarByClass(FileToHbase.class);
		job.setMapperClass(BoobleMapper.class);
		//job.setReducerClass(WordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);// classe clé sortie map
		job.setMapOutputValueClass(Text.class);// classe valeur sortie map    

		job.setInputFormatClass(TextInputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,args[0]);
		
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(0);//Il est bien sur possible de changer cette valeur (1 par défaut)
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		
		final Path outDir = new Path("/tmp");
		FileOutputFormat.setOutputPath(job, outDir);
		/*
		final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
		if (fs.exists(outDir)) { // test si le dossier de sortie existe
			fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
		}
		*/

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
