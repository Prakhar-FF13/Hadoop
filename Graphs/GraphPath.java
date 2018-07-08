import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphPath{
	public static class Node{
		private String id; // Node ID.
		private String neighbours; // ID's of all nodes this node has a connection to.
		private int distance; // Distance of this nodefrom the starting node.
		private String state; // Current node state.

		Node(Text t){ //Parsing Text File representation into Node object.
			String[] parts = t.toString().split("\t");
			this.id = parts[0];
			this.neighbours = parts[1];
			if (parts.length<3 || parts[2].equals("")) 
				this.distance = -1 ;
            		else
				this.distance = Integer.parseInt(parts[2]);
			if (parts.length< 4 || parts[3].equals(""))
				this.state = "P" ;
			else
				this.state = parts[3];
		}
		Node(Text key, Text value){ //Create Node Object from key, value pair.
			this(new Text(key.toString()+"\t"+value.toString()));
		}
		public String getId(){
			return this.id;
		}
		public String getNeighbours(){
			return this.neighbours;
		}
		public int getDistance(){
			return this.distance;
		}
		public String getState(){
			return this.state;
		}
	}
	public static class GraphPathMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Node n = new Node(value);
			if(n.getState().equals("C")){
				context.write(new Text(n.getId()), new Text(n.getNeighbours()+"\t"+n.getDistance()+"\t"+"D"));
				for (String neighbour:n.getNeighbours().split(",")){
					context.write(new Text(neighbour), new Text("\t"+(n.getDistance()+1)+"\tC"));
				}
			}
			else{
				context.write(new Text(n.getId()), new Text(n.getNeighbours()+"\t"+n.getDistance()+"\t"+n.getState()));
			}
		}
	}
	public static class GraphPathReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
			String neighbours = null ;
			int distance = -1 ;
			String state = "P" ;

            		for(Text t: values)
			{
                		Node n = new Node(key, t) ;
				if (n.getState().equals("D")){
					neighbours = n.getNeighbours() ;
                    			distance = n.getDistance() ;
                    			state = n.getState() ;
                    			break ;
				}
				if (n.getNeighbours() != null){
					neighbours = n.getNeighbours();
					if (n.getDistance() > distance)
						distance = n.getDistance() ;
					if (n.getState().equals("D") || (n.getState().equals("C") &&state.equals("P")))
						state=n.getState() ;
				}
			}
			context.write(key, new Text(neighbours+"\t"+distance+"\t"+state));
		}
	}
	public static void main(String[] args) throws Exception{
        	Configuration conf = new Configuration();
		Job job = new Job(conf, "graph path");
        	job.setJarByClass(GraphPath.class);
        	job.setMapperClass(GraphPathMapper.class);
        	job.setReducerClass(GraphPathReducer.class);
        	job.setOutputKeyClass(Text.class);
        	job.setOutputValueClass(Text.class);
        	FileInputFormat.addInputPath(job, new Path(args[0]));
        	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
