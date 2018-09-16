package cosineSimilarity;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class UnigramUnknownMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException{
		
		String[] currentLineArray;
		String[] author;
		String[] date;
		String line;
		String[] tokens;
		String kv_pairs;

		String currentLine = null;
		currentLine = value.toString();
		
		// pre-processing for format 1 ----------------------------
		currentLineArray = currentLine.toString().split("<===>");
		author = currentLineArray[0].toString().split(" ");
		date = currentLineArray[1].toString().split(" ");
		line = currentLineArray[2].toLowerCase(); //System.out.println(line);
		
//		// pre-processing for format 2 ----------------------------
//		line = currentLine.toLowerCase(); //System.out.println(line);
		
		// special characters and space removal -------------------
		line = line.replaceAll("[^A-Za-z0-9\\s]", ""); //System.out.println(line);
		line = line.replaceAll("\\s{2,}", " ").trim(); //System.out.println(line);
		tokens = line.toString().split(" ");
		//System.out.println(author[author.length -1]); //System.out.println(date[date.length -1]);
		
		// for every line .....
		int i = 0;
		while (i < tokens.length){
			if(tokens[i].length() >= 1){
				kv_pairs = author[author.length -1].toLowerCase() + "\t" + tokens[i]; // + "\t" + author[author.length -1].toLowerCase();
				context.write(new Text(kv_pairs), new Text("one"));
			}
			i = i + 1;		
		} // while tokens are available
		
		
	} // void map
} // mapper