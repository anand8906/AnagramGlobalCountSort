import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class AnagramSorter {
	
  public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
  {
	Path tempOutputPath = new Path("/PartBTemp/output");
	Path partitionFilePath = new Path("/PartBTemp/partition");
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "AnagramSorter");
	
	job.setJarByClass(AnagramSorter.class);
	job.setMapperClass(AnagramMapper.class);
	job.setReducerClass(AnagramReducer.class);
	job.setNumReduceTasks(5);
	job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, tempOutputPath);
    
    boolean success = job.waitForCompletion(true) ? true : false;
    
    if(success)
    {
    	  Configuration conf2 = new Configuration();
	  Job job2 = Job.getInstance(conf2, "AnagramSorterFinal");
	  job2.setJarByClass(AnagramSorter.class);
	  job2.setNumReduceTasks(5);
	  FileInputFormat.addInputPath(job2, tempOutputPath);
	  
	  TotalOrderPartitioner.setPartitionFile(job2.getConfiguration(), partitionFilePath);
	  
	  job2.setInputFormatClass(CustomInputFormat.class);
	  job2.setOutputKeyClass(CustomKey.class);
	  job2.setOutputValueClass(Text.class);
	  
	  InputSampler.Sampler<CustomKey, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 4);
      InputSampler.writePartitionFile(job2, sampler);
	  
	  job2.setMapperClass(AnagramMapperFinalSort.class);
	  job2.setReducerClass(AnagramReducerFinalSort.class);
	  job2.setPartitionerClass(TotalOrderPartitioner.class);
	  
	  FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	  
	  System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
    
  }
}