import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class CustomRecordReader extends RecordReader<CustomKey, Text> {
	
	static private int counter = 0;
	private CustomKey key;
	private Text value;
	private LineRecordReader reader = new LineRecordReader();

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public CustomKey getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
       reader.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean nextKeyValue = reader.nextKeyValue();
		if(nextKeyValue)
		{
		  if(key==null)
			key = new CustomKey();
		  if(value == null)
			value = new Text();
		  String record = reader.getCurrentValue().toString();
		  int keyIndex = record.indexOf('\t');
		  String keyStr = record.substring(0, keyIndex) + String.valueOf(counter++);
		  String valueStr = record.substring(keyIndex+1);
		  key.set(keyStr);
		  value.set(valueStr);
		}
		else 
		{
		  key = null;
		  value = null;
		}
		return nextKeyValue;
	}

}
