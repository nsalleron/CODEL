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

import com.google.common.collect.Lists;



public class FileToHbase {

		
	public static class HbaseMapper extends TableMapper<Text, Text>  {

		
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
	   		String stmp ;
			String[] bstmp;
			
 	   		if(value.containsColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ville"))){
				/* Cas de villes IP
				*/
				Text ville = null, IP1 = null , IP2 = null;
				byte[] tmp = null;
				
				/*ville */
				tmp = value.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("ville"));
				ville = new Text("v;" + new String(tmp));
								
				/* IP1 */
	   			tmp = value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("ip1"));
	   			stmp = new String(tmp).replace(".", ";");
	   			bstmp = stmp.split(";");
	   			IP1 = new Text(bstmp[0]+"."+bstmp[1]+".0.0");
	   				   			
	   			/* IP2*/
				tmp = value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("ip2"));
	   			if(tmp != null){
	   				stmp = new String(tmp).replace(".", ";");
		   			bstmp = stmp.split(";");
		   			IP2 = new Text(bstmp[0]+"."+bstmp[1]+".0.0");
				}
	   			if(IP1 != null)
	   				context.write(IP1, ville);
				if(IP2 != null)
					context.write(IP2, ville);
	   			
			}else {
				// Cas de connections FONCTIONNEL
				byte[] tmp = value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("address"));
				String test = new String("");
				if(tmp != null) {
					Text address = new Text(new String(tmp).replace(" ", ""));
					stmp = new String(address.toString()).replace(".", ";");
		   			bstmp = stmp.split(";");
		   			address = new Text(bstmp[0]+"."+bstmp[1]+".0.0");
		   			
					 /* Vraiment moche */
					test += new String(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("date")))+" ";
					test += new String(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("hour")))+" ";
					test += new String(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("address")))+" ";
					if(new String(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("keywords"))).endsWith("+")) {
						test += new String(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("keywords"))).substring(0,
								new String(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("keywords"))).length() -1);
					}else {
						test += new String(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("keywords")));
					}
					Text rowval = new Text(new String("c;"+test));
					
					context.write(address, rowval );
				}
				
			}
	   	}
	}
	
	public static class MyTableReducer extends TableReducer<Text, Text, Text>  {
		
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	
	    	Text wordkey = new Text("TOTO");
	    	String ville = "default";
	        Put end = null;
	        int i =0;
	    	for(Text t : values) {
	    		i++;
	    		String stmp = t.toString();
	    		if(stmp.startsWith("v")) { // Villes
	    			ville = stmp.split(";")[1]; 
	    		}
	    		if(stmp.startsWith("c")) {
	    			end = new Put(DigestUtils.md5(stmp.split(";")[1]));
	    		}
	    	}   	
	    	if(end != null ) {
	    		end.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("locality"), Bytes.toBytes(ville));
	    		context.write(wordkey, end);
	    	}
	   	}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		Job job = Job.getInstance(conf,"job");
		
		List<Scan> scans = new ArrayList<Scan>();
		Scan connexions = new Scan();
		connexions.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("connections"));
		scans.add(connexions);
		
		Scan villes_ip = new Scan();
		villes_ip.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("villes_ip"));
		scans.add(villes_ip);

		TableMapReduceUtil.initTableMapperJob(
				scans,
				HbaseMapper.class,
				Text.class,
				Text.class,
				job);
		
		job.setReducerClass(MyTableReducer.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "connections");
		job.setJarByClass(FileToHbase.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setNumReduceTasks(3);
		
		System.out.println("Done");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
