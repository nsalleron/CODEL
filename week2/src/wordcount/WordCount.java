package wordcount;

import java.io.IOException;
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

public class WordCount {

  /* Mapper */
  public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1); /* Entier */
    private Text word = new Text();	 /* Equivalent String */
      
    public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) { 
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class WordCountReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("mapreduce.map.speculative", false);
	conf.setBoolean("mapreduce.reduce.speculative", false);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setMapOutputKeyClass(Text.class);// classe clé sortie map
    job.setMapOutputValueClass(IntWritable.class);// classe valeur sortie map    
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
