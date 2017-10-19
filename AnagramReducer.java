import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducer extends Reducer<Text,Text, LongWritable,Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
	{
	  int count = 0;
	  StringBuilder sb = new StringBuilder("");
	  for(Text t : values)
      {
	    sb.append(t.toString());
	    sb.append(" ");
	    count++;
	  }
		
	  LongWritable outKey = new LongWritable();
	  outKey.set(count);
	  Text outValue = new Text();
      outValue.set(sb.toString());
	  
      context.write(outKey, outValue);
	}
	
}
