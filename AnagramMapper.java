import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnagramMapper extends Mapper<Object, Text, Text, Text> {

  public void map(Object key, Text value, Context context) 
		  throws IOException, InterruptedException
  {
	  Text sortedWord =  new Text();
	  char[] inChars = value.toString().toCharArray();
	  Arrays.sort(inChars);
	  sortedWord.set(new String(inChars));
	  context.write(sortedWord, value);
  }
}
