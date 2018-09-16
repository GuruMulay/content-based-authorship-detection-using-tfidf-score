package authorAttrVect;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuthAttrVectMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException{
		
		String[] authorWordNAuthTFArray = null;
		String totalAuthors = null;
		int authorsN = 0; // total number of authors in the dataset
		
		double df = 0; // document frequency
		double idf = 0; // inverse document frequency
		double tf_idf = 0; // tf X idf
		
		String currentLine = null;
		currentLine = value.toString();
		authorWordNAuthTFArray = currentLine.split("\t", 4);
		
		Configuration conf = context.getConfiguration();
		totalAuthors = conf.get("authorsN").toString();
		authorsN = Integer.parseInt(totalAuthors);
		
//		authorsN = 6;  // hard-coded N
		
		df = (double)((double)authorsN/Double.valueOf(authorWordNAuthTFArray[2]));
		idf = (double)(Math.log10(df));
		tf_idf = Double.valueOf(authorWordNAuthTFArray[3])*idf;
				
		// for every line ..... sends <author, (word:tf:idf:tf_idf)>
		context.write(new Text(authorWordNAuthTFArray[0]), new Text(authorWordNAuthTFArray[1] + ":" + authorWordNAuthTFArray[3] + ":" + idf + ":" + tf_idf));
		
	} // void map
} // mapper