package authorAttrVect;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorNameReducer extends Reducer<Text,Text,Text,NullWritable>{
	// hadoop counter to count the number of authors N
//	public static enum authorCount {count}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
//		int maxCount = 0; /// stores maxCount for among all values of a particular author (key)
//		int currentCount = 0;
		
//		Map<String, Integer>authorHashSet = new HashMap<String, Integer>();
//		Map<String, Integer>wordHashSet = new HashMap<String, Integer>();
		
//		int count = 0;
//		for(Text val: values) {
//			count++;
//		}		
		context.write(key, NullWritable.get());

		
//		if(!(authorHashSet.containsKey(key))){
//			authorHashSet.put(key.toString(), 1);
//			context.getCounter(authorCount.count).increment(1);
//		} // count total number of authors N
//		
		
//		for(Text val:values){
//			String[] wordCount = val.toString().split(":");
//			wordHashSet.put(wordCount[0], Integer.valueOf(wordCount[1]));
//			currentCount = Integer.parseInt(wordCount[1]);
//
//			if(currentCount > maxCount)
//			{
//				maxCount = currentCount;
//			} // store max count among all the values
//		
//		} // for val in values 
//		
//
//		for(String keyWord : wordHashSet.keySet()){
//			String termFreq = double.toString((double)(0.5 + 0.5*(double)wordHashSet.get(keyWord)/maxCount));
//			String wordCountMaxCountTF = keyWord + "\t" + wordHashSet.get(keyWord) + "\t"
//					+ maxCount + "\t" + termFreq;
//			context.write(key, new Text(wordCountMaxCountTF));
//		} 

	} // reduce
}