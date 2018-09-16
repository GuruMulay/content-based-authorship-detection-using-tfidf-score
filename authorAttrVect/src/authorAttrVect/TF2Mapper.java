package authorAttrVect;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TF2Mapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException{
		
		String[] authorWordCountMaxTFArray = null;
		
		String currentLine = null;
		currentLine = value.toString();
		authorWordCountMaxTFArray = currentLine.split("\t", 5);
		
		// for every line ..... sends <word, (author:tf)>
		context.write(new Text(authorWordCountMaxTFArray[1]), new Text(authorWordCountMaxTFArray[0] + ":" + authorWordCountMaxTFArray[4]));
		
	} // void map
} // mapper