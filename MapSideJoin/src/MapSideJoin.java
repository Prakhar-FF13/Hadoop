import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoin {
	
	public static class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputValue = new Text(); 
		private Map<String,String> custAddressInfo = new HashMap<String,String>();
		
		
		protected void setup(Context context) throws IOException{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p: files){
				if("users.txt".equals(p.getName())){
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null){
						String[] tokens = line.split(",");
						String id = tokens[0];
						String nameAddress = tokens[1] + " " + tokens[2];
						
						custAddressInfo.put(id, nameAddress);
						line = reader.readLine();
					}
					reader.close();
				}
			}
		}
		
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] tokens = line.split(",");
			String custId = tokens[2];
			String address = custAddressInfo.get(custId);
			outputKey.set(custId);
			outputValue.set("amount = "+tokens[3] +  " & product = "+tokens[4] +" & address ="+ address);
			context.write(outputKey, outputValue);
		}
	}

	public static void main(String args[]) throws Exception, URISyntaxException{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Map Side Join");
		job.setNumReduceTasks(0);
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapJoinMapper.class);
		DistributedCache.addCacheFile(new URI("users.txt"), job.getConfiguration());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		
	}
}
