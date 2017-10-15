package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
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

		
	public static class HbaseMapper extends TableMapper<Text, Text>  {

		
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
	   		
	   		if(value.containsColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ville"))){
				/* Cas de villes IP
				Put obj = new Put(DigestUtils.md5(value));
				obj.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(t[i]), Bytes.toBytes(itr.nextToken()));
				context.write(wordkey, obj);
				i++;
				*/
	   			Text ville = new Text(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("ville")));
	   			Text IP1 = new Text(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("ip1")));
	   			Text IP2 = new Text(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("ip2")));
	   			
	   			context.write(IP1, ville);
	   			context.write(IP2, ville);
	   			
			}else {
				// Cas de connections
				
				Text address = new Text(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("address")));
				
				String test = new String();
				test += value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("date"))+"_";
				test += value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("hour"))+"_";
				test += value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("address"))+"_";
				test += value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("keywords"));
				
	
				Text key = new Text(";"+DigestUtils.md5(test));
				context.write(address, key);
			}
	   	}
	}
	
	public static class MyTableReducer extends TableReducer<Text, Text, Text>  {

	 	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    		int i = 0;
	    		Text wordkey = new Text("TOTO");
	    		
	    		if(key.toString().startsWith(";")) {	// Connexions
	    			Put obj = new Put(key.toString().split(";")[1].getBytes());
					obj.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("locality"), Bytes.toBytes(key.toString()));
					context.write(wordkey, obj);
	    		}
	   	}
	}

	public static void main(String[] args) throws Exception {
		
		
		
		List<Scan> scans = new ArrayList<Scan>();
		Scan connexions = new Scan();
		connexions.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("connections"));
		scans.add(connexions);
		
		Scan villes_ip = new Scan();
		villes_ip.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("villes_ip"));
		scans.add(villes_ip);
		
		Configuration conf =new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(FileToHbase.class);
		
		TableMapReduceUtil.initTableMapperJob(
				scans,
				TableMapper.class,
				Text.class,
				Text.class,
				job);
		
		TableMapReduceUtil.initTableReducerJob("connections", MyTableReducer.class, job);
		
		
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "connections");
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		
		
		//final Path outDir = new Path("/tmp");
		//FileOutputFormat.setOutputPath(job, outDir);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
