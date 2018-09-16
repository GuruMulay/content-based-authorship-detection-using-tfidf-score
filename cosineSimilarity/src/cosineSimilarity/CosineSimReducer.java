package cosineSimilarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static java.lang.Math.sqrt;


public class CosineSimReducer extends Reducer<Text,Text,Text,Text>{
	
	public static Map<String, Double> authorSim = new HashMap<String, Double>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// <author (word:tfidf:tfidfUnkn)>
		
		double prod = 0;
		double ASq = 0;
		double BSq = 0;
		double sumAB = 0;
		double sumASq = 0;
		double sumBSq = 0;
		double cosSim = 0;
		String line = null;
		String line2 = null;
		String[] tokens = null;
	
		
		for(Text val:values) {  // for (word:tfidf:tfidfUnkn)
			String[] tfidf = val.toString().split(":", 3);
			
			// sums over all words
			prod = Double.valueOf(tfidf[1])*Double.valueOf(tfidf[2]);
			sumAB = sumAB + prod;
			
			ASq = Double.valueOf(tfidf[1])*Double.valueOf(tfidf[1]);
			sumASq = sumASq + ASq;
			
			BSq = Double.valueOf(tfidf[2])*Double.valueOf(tfidf[2]);
			sumBSq = sumBSq + BSq;
			
		}  // for all words = keys
		
		cosSim = (double)sumAB/((double)sqrt(sumASq)*(double)sqrt(sumBSq));
		
		context.write(key, new Text(Double.toString(cosSim)));
		
		
	} // reduce
}