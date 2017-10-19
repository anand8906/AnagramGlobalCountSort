import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducerFinalSort extends Reducer<CustomKey,Text, NullWritable,Text> {
	
	//private Map<Long, List<Text> > anagramCounts = new TreeMap<>();
	
	public void reduce(CustomKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
	{
		
	  /*
	  if(!(anagramCounts.containsKey(key.get())))
	  {
		List<Text> newList = new ArrayList<Text>();
		anagramCounts.put(key.get(), newList);
	  }
	  for(Text value : values)
	  {
		anagramCounts.get(key.get()).add(value);
	  }
	  */
	
	  /*
	  List<Text> newList = new ArrayList<Text>();
	  for(Text value : values)
		  newList.add(value);
	  anagramCounts.put(key.get(), newList);
	  */
	  for(Text value : values)
		 context.write(NullWritable.get(), value);;
	}
	
	/*
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		ArrayList<Long> keys = new ArrayList<Long>(anagramCounts.keySet());
        for(int i= keys.size()-1; i >= 0; i--)
        {
        	   for(Text tx : anagramCounts.get(keys.get(i)))
        		   context.write(NullWritable.get(), tx);
        }
	}
	*/
}
