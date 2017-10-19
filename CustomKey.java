import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable{
	
	Text attr;

	public CustomKey(Text init)
	{
	  set(init);	
	}

	public CustomKey()
	{
	  set(new Text());	
	}
	
    public void set(Text init) 
    {
	  this.attr = init;
	}
    
    public void set(String init) 
    {
	  this.attr.set(init);;
	}
    
    public String get() 
    {
	  return this.attr.toString();
	}
    
    private Text getAttr()
    {
      return attr;    	
    }
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
	  attr.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	  attr.write(arg0);	
	}

	@Override
	public int compareTo(Object o) {
		return (-1 * attr.compareTo(((CustomKey) o).getAttr()));
	}

}
