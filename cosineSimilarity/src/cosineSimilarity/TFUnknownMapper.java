package cosineSimilarity;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFUnknownMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException{
		
		String[] authorWordCountArray = null;
		
		String currentLine = null;
		currentLine = value.toString();
		authorWordCountArray = currentLine.split("\t", 3);
		
		// for every line ..... sends <author, (word:count)>
		context.write(new Text(authorWordCountArray[0]), new Text(authorWordCountArray[1] + ":" + authorWordCountArray[2]));
		
	} // void map
} // mapper