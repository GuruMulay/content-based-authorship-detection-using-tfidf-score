package cosineSimilarity;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException{
		
//		String[] authorCosSimArray = null;
//		
//		String currentLine = null;
//		currentLine = value.toString();
//		authorCosSimArray = currentLine.split("\t", 2);
		
		// for every line ..... sends < "", (author	cosSim)>
		context.write(new Text(""), new Text(value));
		
	} // void map
} // mapper