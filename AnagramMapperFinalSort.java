import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnagramMapperFinalSort extends Mapper<CustomKey, Text, CustomKey, Text> {

  public void map(CustomKey key, Text value, Context context) 
		  throws IOException, InterruptedException
  {
	  context.write(key, value);
  }
}
