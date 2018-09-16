package authorAttrVect;

import java.io.BufferedReader;
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

public class TF2Reducer extends Reducer<Text,Text,Text,Text>{
	
	public static ArrayList<String> authors = new ArrayList<String>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// receives <word, (author:tf)>
		int nAuthors = 0; // number of authors using the particular word
		String authKey = null;
		int temp = 0;
		
		Map<String, String>wordHashSet = new HashMap<String, String>();
		
		// read author names file ----------------------------------
		String line = null;
		int authorsN = authors.size();
		
		if (authorsN==0){
			Path path = new Path(context.getConfiguration().get("author_f"));
			FileSystem f = FileSystem.get(new Configuration());

			BufferedReader reader = new BufferedReader(new InputStreamReader(f.open(path)));
//			line = reader.readLine();

			while ((line = reader.readLine()) != null){
				authors.add(line);
			} // while
			reader.close();
		} // if

		
		for(Text val:values){
			String[] authorTF = val.toString().split(":");
			nAuthors += 1;
			
			wordHashSet.put(authorTF[0], authorTF[1]); // (author:tf)
			
		} // for val in values 
			
		
		for(String authName : authors){
		    if(wordHashSet.containsKey(authName.toString())){
				// System.out.println("FOUND !!!!!!");
		    	temp = 0;
		    }
		    else {
		    	// System.out.println("NOT FOUND !!!!!!");
		    	wordHashSet.put(authName.toString(), "0.500000000000000000"); // (author:tf = 0.5)
		    }
		} // for
		
		
		
//		for(Text val:values){
//			String[] authorTF = val.toString().split(":");
//			context.write(new Text(authorTF[0]), new Text(key + "\t" + nAuthors + "\t" + authorTF[1]));
//		} // for val in values 

		
		for(String keyWord : wordHashSet.keySet()){
			authKey = keyWord;
			context.write(new Text(authKey), new Text(key + "\t" + nAuthors + "\t" + wordHashSet.get(keyWord)));
		} 
		// authorWordNAuthTF
	} // reduce
}