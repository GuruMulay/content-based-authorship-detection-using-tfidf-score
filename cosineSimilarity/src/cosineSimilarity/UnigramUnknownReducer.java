package cosineSimilarity;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnigramUnknownReducer extends Reducer<Text,Text,Text,IntWritable>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for(Text val: values) {
			count++;
		}		
		context.write(key, new IntWritable(count));

	}
}