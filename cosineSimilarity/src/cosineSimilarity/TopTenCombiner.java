package cosineSimilarity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenCombiner extends Reducer<Text,Text,Text,Text>{
	
	public static TreeMap <Double, String> cosSim = new TreeMap<Double, String>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int maxCount = 0; /// stores maxCount for among all values of a particular author (key)
		int currentCount = 0;
		
		for(Text val:values){
			String[] authCosSim = val.toString().split("\t", 2);
			
			cosSim.put(Double.valueOf(authCosSim[1]), authCosSim[0].toString());
			
			// remove the last key which will be the lowest
			if (cosSim.size() > 10) {
				cosSim.remove(cosSim.firstKey());
			}
			
		} // for val in values 
		
		
//		for(String keyWord : wordHashSet.keySet()){
//			String termFreq = double.toString((double)(0.5 + 0.5*(double)wordHashSet.get(keyWord)/maxCount));
//			String wordCountMaxCountTF = keyWord + "\t" + wordHashSet.get(keyWord) + "\t"
//					+ maxCount + "\t" + termFreq;
//			context.write(key, new Text(wordCountMaxCountTF));
//		} 
		
		// output to 10 cosSim -----------------------------------------
		for (String keyWord : cosSim.descendingMap().values()){
			context.write(new Text(""), new Text(keyWord.toString()));
		}

	} // reduce
}