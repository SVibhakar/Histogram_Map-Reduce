import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;
    
class Color implements WritableComparable<Color> {
	public int type;       
	public int intensity;  
	Color() {}
	Color (int type, int value) {
		this.type=type;
		this.intensity=value;
	}
	public String toString() {
		return (this.type + " " + this.intensity);
	}
	public void write (DataOutput output) throws IOException {
		output.writeInt(this.type);
		output.writeInt(this.intensity);
	}
	public void readFields (DataInput input) throws IOException {
		type = input.readInt();
		intensity = input.readInt();
	}
	public int compareTo (Color key) {
		int color1 = this.type;
		int color2 = key.type;
		int intensity1 = this.intensity;
		int intensity2 = key.intensity;
		if(color1 == color2) 
		{
			if(intensity1 > intensity2)
			        return 1;
			else if (intensity1 == intensity2)
			        return 0;
			else
			        return -1;
	        }
		else
		{
			if(color1 > color2)
				return 1;
			else
			        return -1;
	        }
	}
}

public class Histogram {
	public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> 
	{   	@Override
		public void map ( Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			int red=0, blue=0, green=0;
			Scanner sc = new Scanner(value.toString()).useDelimiter(",");
			red = sc.nextInt();
		        green = sc.nextInt();
		        blue = sc.nextInt(); 
		        context.write(new Color(1,red), new IntWritable(1));
		        context.write(new Color(2,green), new IntWritable(1));
		        context.write(new Color(3,blue), new IntWritable(1));
		        sc.close();
		}
	}
	public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> 
	{   	@Override
		public void reduce ( Color key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable v : values){
				sum += v.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
	public static void main ( String[] args ) throws Exception{
		Job job = Job.getInstance();
		job.setJobName("MyJob");
		job.setJarByClass(Histogram.class);
		job.setOutputKeyClass(Color.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Color.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(HistogramMapper.class);
		job.setReducerClass(HistogramReducer.class);  
		job.setCombinerClass(HistogramCombiner.class);      
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
	}
}

