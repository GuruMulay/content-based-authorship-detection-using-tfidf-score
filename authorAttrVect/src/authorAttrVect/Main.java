package authorAttrVect;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;   // don't use .mapred.

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import authorAttrVect.TF1Reducer.authorCount;

public class Main {
	public static final String MR1_output_path = "mr1output";
	public static final String MR2_output_path = "mr2output";
	public static final String author_name_output = "author_name_output";
	public static final String MR3_output_path = "mr3output";
	public static final String aav_output_path = "aav_output";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException {
		
		if (args.length != 2) {
			System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration(); // hadoop config file
		
//		conf.set("author_f", "hdfs://phoenix:30201" + "/user/guru5/author_name_output/part-r-00000" );
		
		// MapReduce Job 1 -----------------------------------------------
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(Main.class);
		
		job1.setMapperClass(UnigramCountMapper.class);
		job1.setReducerClass(UnigramCountReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(MR1_output_path));
		if(job1.waitForCompletion(true)){
			System.out.println("Job 1 finished!");
		}
		else{
			System.out.println("Job 1 failed!");
		}
		
		
		// MapReduce Job 2 -----------------------------------------------
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(Main.class);
		
		job2.setMapperClass(TF1Mapper.class);
		job2.setReducerClass(TF1Reducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(MR1_output_path));
		FileOutputFormat.setOutputPath(job2, new Path(MR2_output_path));
		if(job2.waitForCompletion(true)){
			System.out.println("Job 2 finished!");
		}
		else{
			System.out.println("Job 2 failed!");
		}
		
		// get the total number of authors using hadoop internal counter ------
		Counters counters = job2.getCounters();
		Counter counter = counters.findCounter(authorCount.count);
		conf.set("authorsN", String.valueOf(counter.getValue()));
		
		
		// MapReduce Job 2b -----------------------------------------------
		Job job2b = Job.getInstance(conf);
		job2b.setJarByClass(Main.class);
		
		job2b.setMapperClass(AuthorNameMapper.class);
		job2b.setReducerClass(AuthorNameReducer.class);
		
		job2b.setOutputKeyClass(Text.class);
		job2b.setOutputValueClass(Text.class);
		job2b.setInputFormatClass(TextInputFormat.class);
		job2b.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2b, new Path(MR1_output_path));
		FileOutputFormat.setOutputPath(job2b, new Path(author_name_output));
		if(job2b.waitForCompletion(true)){
			System.out.println("Job 2b finished!");
		}
		else{
			System.out.println("Job 2b failed!");
		}
		
		
		
		// pre-processing ------------------------------------------------
		conf.set("author_f", "hdfs://phoenix:30201" + "/user/guru5/author_name_output/part-r-00000" );
//		conf.set("author_f", "hdfs://phoenix:30201" + "/dataLoc/auth12.txt" );
		
		
		// MapReduce Job 3 -----------------------------------------------
		Job job3 = Job.getInstance(conf);
		job3.setJarByClass(Main.class);

		job3.setMapperClass(TF2Mapper.class);
		job3.setReducerClass(TF2Reducer.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(MR2_output_path));
		FileOutputFormat.setOutputPath(job3, new Path(MR3_output_path));
		if(job3.waitForCompletion(true)){
			System.out.println("Job 3 finished!");
		}
		else{
			System.out.println("Job 3 failed!");
		}
		
		
		// MapReduce Job 4 -----------------------------------------------
		Job job4 = Job.getInstance(conf);
		job4.setJarByClass(Main.class);

		job4.setMapperClass(AuthAttrVectMapper.class);
		job4.setReducerClass(AuthAttrVectReducer.class);

		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job4, new Path(MR3_output_path));
		FileOutputFormat.setOutputPath(job4, new Path(aav_output_path));
		if(job4.waitForCompletion(true)){
			System.out.println("Job 4 finished!");
		}
		else{
			System.out.println("Job 4 failed!");
		}

		
		
//		FileOutputFormat.setOutputPath(job_, new Path(args[1]));	
//		System.exit(job_.waitForCompletion(true) ? 0 : 1);
	} // main
}

