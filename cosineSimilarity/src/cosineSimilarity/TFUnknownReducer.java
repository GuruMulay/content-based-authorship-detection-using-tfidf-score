package cosineSimilarity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFUnknownReducer extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// receives <author, (word:count)>
		int maxCount = 0; /// stores maxCount for among all values of a particular author (key)
		int currentCount = 0;
		
		Map<String, Integer>wordHashSet = new HashMap<String, Integer>();
			
		for(Text val:values){
			String[] wordCount = val.toString().split(":");
			wordHashSet.put(wordCount[0], Integer.valueOf(wordCount[1]));
			currentCount = Integer.parseInt(wordCount[1]);

			if(currentCount > maxCount)
			{
				maxCount = currentCount;
			} // store max count among all the values
		
		} // for val in values 
		
		
		for(String keyWord : wordHashSet.keySet()){
			String termFreq = Double.toString((double)((double)0.5 + (double)0.5*(double)wordHashSet.get(keyWord)/(double)maxCount));
			String wordCountMaxCountTF = keyWord + "\t" + wordHashSet.get(keyWord) + "\t"
					+ maxCount + "\t" + termFreq;
			context.write(key, new Text(wordCountMaxCountTF));
		} 

	} // reduce
}