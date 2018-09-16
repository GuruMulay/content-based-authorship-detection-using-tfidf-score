package cosineSimilarity;

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
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {
	public static final String author_name_output = "author_name_output";
	public static final String MCR1_output_path = "mcr1output";
	public static final String unk_tf_output = "unk_tf_output";
	public static final String unk_cosine_output = "unk_cosine_output";
//	@SuppressWarnings("deprecation")
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException {
		
		if (args.length != 3) {
			System.out.printf("Usage: <jar file> <input1 (unknown author text) dir> <input2 (aav vector file) dir> <output dir>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration(); // hadoop config file
		
//		conf.set("author_f", "hdfs://phoenix:30201" + "/dataLoc/auth.txt" );
//		conf.set("author_f", "hdfs://phoenix:30201" + "/user/guru5/author_name_output/part-r-00000" );
//		conf.setInt("N_auth", 6);
		
		conf.set("unkAuth", "hdfs://phoenix:30201" + "/user/guru5/unk_tf_output/part-r-00000" );
		
		// MapCombineReduce Job 1 -----------------------------------------------
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(Main.class);
		
//		job1.setMapperClass(CosineSimMapper.class);
//		job1.setReducerClass(CosineSimCombiner.class);
//		job1.setReducerClass(CosineSimReducer.class);
		
		job1.setMapperClass(UnigramUnknownMapper.class);
		job1.setReducerClass(UnigramUnknownReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(MCR1_output_path));
//		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		if(job1.waitForCompletion(true)){
			System.out.println("Job 1 finished!");
		}
		else{
			System.out.println("Job 1 failed!");
		}
		
		
		// MapReduce Job 2 -----------------------------------------------
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(Main.class);
		
		job2.setMapperClass(TFUnknownMapper.class);
		job2.setReducerClass(TFUnknownReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(MCR1_output_path));
		FileOutputFormat.setOutputPath(job2, new Path(unk_tf_output));
		if(job2.waitForCompletion(true)){
			System.out.println("Job 2 finished!");
		}
		else{
			System.out.println("Job 2 failed!");
		}

//		FileSystem.delete(Path f, boolean recursive);
		
//		// get the total number of authors using hadoop internal counter ------
//		Counters counters = job2.getCounters();
//		Counter counter = counters.findCounter(authorCount.count);
//		conf.set("authorsN", String.valueOf(counter.getValue()));
		
		
		// MapReduce Job 3 -----------------------------------------------
		Job job3 = Job.getInstance(conf);
		job3.setJarByClass(Main.class);

		job3.setMapperClass(CosineSimMapper.class);
		job3.setReducerClass(CosineSimReducer.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		DistributedCache.addCacheFile(new Path(unk_tf_output + "/part-r-00000").toUri(), job3.getConfiguration());
		FileInputFormat.setInputPaths(job3, new Path(args[1]));  // aav vector file
		FileOutputFormat.setOutputPath(job3, new Path(unk_cosine_output));  // output file
		if(job3.waitForCompletion(true)){
			System.out.println("Job 3 finished!");
		}
		else{
			System.out.println("Job 3 failed!");
		}
		
		
		// MapReduce Job 4 -----------------------------------------------
		Job job4 = Job.getInstance(conf);
		job4.setJarByClass(Main.class);

		job4.setMapperClass(TopTenMapper.class);
//		job4.setReducerClass(TopTenReducer.class);
		job4.setCombinerClass(TopTenCombiner.class);

		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job4, new Path(unk_cosine_output + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job4, new Path(args[2]));
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

