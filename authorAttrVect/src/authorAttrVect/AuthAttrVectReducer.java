package authorAttrVect;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthAttrVectReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// receives <author, (word:tf:idf:tf_idf)>
		String aav = null; // author attribute vector
		
		Map<String, Integer>authAtrrVect = new HashMap<String, Integer>();
		
		
		for(Text val:values){
			String[] wordTfIdfTFIDFArray = val.toString().split(":", 4);
			aav = wordTfIdfTFIDFArray[0] + "\t" + wordTfIdfTFIDFArray[1] + "\t" + wordTfIdfTFIDFArray[2] + "\t" + wordTfIdfTFIDFArray[3];
			authAtrrVect.put(aav, 1);
		} // for val in values 
		
		
//		for(Text val:values){
//			String[] authorTF = val.toString().split(":");
//			context.write(new Text(authorTF[0]), new Text(key + "\t" + nAuthors + "\t" + authorTF[1]));
//		} // for val in values 

		
		for(String keyAAV : authAtrrVect.keySet()){
			context.write(new Text(key), new Text(keyAAV));
		} 

	} // reduce
}