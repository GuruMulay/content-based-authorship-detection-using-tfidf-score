package authorAttrVect;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

public class UnigramCountReducer extends Reducer<Text,Text,Text,IntWritable>{
	
//	 private MultipleOutputs mos;
//	 @Overirde
//	 public void setup(Context context) {
//		 mos = new MultipleOutputs(context);
//	 }
	 
//	 private MultipleOutputs<Text, IntWritable> mos;
//     private Text outputKey = new Text();
//     private IntWritable outputValue = new IntWritable();
//    
//     @Override
//     public void setup(Context context) throws IOException, InterruptedException {
//               mos = new MultipleOutputs<Text,IntWritable>(context);
//}
     
//	private MultipleOutputs<Text,Text> multipleOutputs;
//	public void setup(Context context) throws IOException, InterruptedException
//	{
//		multipleOutputs = new MultipleOutputs<Text, Text>(context);
//	}
//	 
     
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {
		int maxCount = 0;
		int maxCount2 = 11111;
		
		int count = 0;
		for(Text val: values) {
			count++;
		}
		
		
//		outputKey.set("maxCount");
//		outputValue.set(maxCount2);
//		mos.write("part-r-00000", outputKey, outputValue);
		
		context.write(key, new IntWritable(count));
//		context.write(key, new IntWritable(maxCount));
	}
}