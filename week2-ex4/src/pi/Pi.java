package pi;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
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



public class Pi {
	
	

  /* Mapper */																// !-On change le mapper
  public static class BoobleMapper extends Mapper<LongWritable, Text, Text, Text>{
   
    private Text wordkey = new Text();	 /* Equivalent String */
    private Text word = new Text();
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	Random generator = new Random();
    	Double xGraine = Double.parseDouble(value.toString().split(";")[0]);
    	Double yGraine = Double.parseDouble(value.toString().split(";")[1]);
    	wordkey.set("toto");
    	for(int i = 0 ; i< 100 ; i++) {
    		
    		Double x = 0 + (xGraine - 0)*generator.nextDouble();
    		Double y = 0 + (yGraine - 0)*generator.nextDouble();
    		
    		word.set(x+";"+y);
    		context.write(wordkey,word );
    	}

    }
  }
  
  public static class WordCountReducer 
       extends Reducer<Text,Text,Text,Text> {
   
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	double dansCercle = 0;
    	double horsCercle = 0;
    	double finalResult = 0;
    	Text resultText = new Text();
    	
    	for(Text val : values) {
    		String value = val.toString();
    		Double x = Double.parseDouble(value.split(";")[0]);
    		Double y = Double.parseDouble(value.split(";")[1]);
    		//resultText.set("X : "+x+" Y : "+y);
    		//context.write(key, resultText);
    		Double result = Math.sqrt(Math.pow((0-x), 2) + Math.pow((0-y),2));
    		if(result > 50) {
    			horsCercle++;
    		}else {
    			dansCercle++;
    		}
    	}
    	
    	finalResult = (4*dansCercle)/(horsCercle+dansCercle);
    	resultText.set("Estimate Pi : "+finalResult);
    	context.write(key, resultText);

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
    job.setJarByClass(Pi.class);
    job.setMapperClass(BoobleMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setMapOutputKeyClass(Text.class);// classe clé sortie map
    job.setMapOutputValueClass(Text.class);// classe valeur sortie map    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
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
