package authorAttrVect;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuthorNameMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException{
		
		String[] authorWordCountArray = null;
		
		String currentLine = null;
		currentLine = value.toString();
		authorWordCountArray = currentLine.split("\t", 3);
		
		// for every line ..... sends <author, "one">
		context.write(new Text(authorWordCountArray[0]), new Text("one"));
		
	} // void map
} // mapper