package booble;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class Booble {

  /* Mapper */																// !-On change le mapper
  public static class BoobleMapper extends Mapper<LongWritable, Text, Text, Text>{
   
    private Text wordkey = new Text();	 /* Equivalent String */
    private Text words = new Text();
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	//On doit extraire la tranche horaire.  
    	/*
    	 * WHILE TOKEN
    	 * 		TAKE TOKEN HEURE
    	 * 			SPLIT HEURE WITH MINUTE
    	 *				PARSEMINUTE()
    	 *					if0<MINUTE<30
    	 *						
    	 *					else // MINUTE 30-59
    	 */
    	
    	StringTokenizer itr = new StringTokenizer(value.toString()," ");
    	
        while (itr.hasMoreTokens()) { 
        	itr.nextToken();	//DATE
        	String heure = itr.nextToken(); //HEURE
        	String valHeure = heure.split("_")[0];
        	int valMinute = Integer.parseInt(heure.split("_")[1]);
        	itr.nextToken(); // IP
        	String mots = itr.nextToken();
        	if(valMinute <30) {
        		wordkey.set(valHeure+','+0);	
        	}else {
        		wordkey.set(valHeure+','+30);
        		
        	}
        	if(!mots.contains("+"))
        		mots += "+";
        	words.set(mots);
        	context.write(wordkey, words);
        }

    }
  }
  
  public static class WordCountReducer 
       extends Reducer<Text,Text,Text,Text> {
   
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	
    
    	String t = key.toString();
    	String heure = t.split(",")[0];
    	String tranche = t.split(",")[1];
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
    	
    	if(Integer.parseInt(tranche)<30){
    		resultat = new Text();
    		resultat.set("entre "+heure+":00 et "+heure+":30 "+res +" nbtot : " +max);
    		context.write(key, resultat);
    	}else {
    		resultat = new Text();
    		resultat.set("entre "+heure+":30 et "+heure+":59 "+res +" nbtot : "+ max);
    		context.write(key, resultat);
    	}

    }
  }

  
  
 
  
  
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("mapreduce.map.speculative", false);
	conf.setBoolean("mapreduce.reduce.speculative", false);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: booble <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "booble count");
    job.setJarByClass(Booble.class);
    job.setMapperClass(BoobleMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setMapOutputKeyClass(Text.class);// classe clé sortie map
    job.setMapOutputValueClass(Text.class);// classe valeur sortie map    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    //job.setPartitionerClass(HashPartitioner.class);//partitioner
    job.setNumReduceTasks(1);//Il est bien sur possible de changer cette valeur (1 par défaut)
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    final Path outDir = new Path(otherArgs[1]);
    FileOutputFormat.setOutputPath(job, outDir);
    final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
	 if (fs.exists(outDir)) { // test si le dossier de sortie existe
		 fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
	 }
   
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
