package cosineSimilarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class CosineSimMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static Path[] cacheFile = new Path[2];
	public static Map<String,String> unknownAuthor = new HashMap<String,String>();
	
//	@SuppressWarnings("deprecation")
	public void setup(Context context) throws IOException{
		
		String[] tokens = null;
		String unkLine = null; 
		Configuration conf = context.getConfiguration();
		
		cacheFile = DistributedCache.getLocalCacheFiles(conf);
		BufferedReader reader = new BufferedReader(new FileReader(cacheFile[0].toString()));
		unkLine = reader.readLine();
		
		while(unkLine != null){
			tokens = unkLine.split("\t", 5); 
			// unknown    over	frq=1	max=4	tf=0.625
			// stores <word, tf>
			unknownAuthor.put(tokens[1].toString(), tokens[4].toString());
			unkLine = reader.readLine();
			
		} // while
		
	} // setup
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
		String[] authorWordTfIdfTFIDFArray = null;

		String currentLine = null;
		String author = null;
		String word = null;
		double tf = 0;
		double idf = 0;
		double tfidf = 0;
		double tfidfUnkn = 0;
		
		currentLine = value.toString();
		// twain	five	0.625	0.7781513	0.48634455
		authorWordTfIdfTFIDFArray = currentLine.split("\t", 5);
		
		author = authorWordTfIdfTFIDFArray[0];
		word = authorWordTfIdfTFIDFArray[1];
		tf = Double.valueOf(authorWordTfIdfTFIDFArray[2]);
		idf = Double.valueOf(authorWordTfIdfTFIDFArray[3]);
		tfidf = Double.valueOf(authorWordTfIdfTFIDFArray[4]);
		
		if(unknownAuthor.containsKey(word.toString())){
			tfidfUnkn = (double)idf*Double.valueOf(unknownAuthor.get(word.toString()));
		}
		
		else {
			tfidfUnkn = (double)idf*(double)0.5;
		} 
	
		// for every line ..... sends <word, (author:tf:idf:tfidf)>
		// context.write(new Text(word), new Text(author + ":" + tf + ":" + idf + ":" + tfidf));
		context.write(new Text(author), new Text(word + ":" + tfidf + ":" + tfidfUnkn));  ///// FORGOT WORD !!!!! ===> 0.9999999

	} // void map
} // mapper
