package vm;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.Mapper.Context;

  public class MySummaryJob {
	public static class MyMapper extends TableMapper<Text, IntWritable>  {
		  public static final byte[] CF = "cf".getBytes();
		  public static final byte[] ATTR1 = "attr1".getBytes();

		  private final IntWritable ONE = new IntWritable(1);
		  private Text text = new Text();

		  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		    String val = new String(value.getValue(CF, ATTR1));
		    text.set(val);     // we can only emit Writables...
		    context.write(text, ONE);
		  }
		}
	public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {
		  public static final byte[] CF = "cf".getBytes();
		  public static final byte[] COUNT = "count".getBytes();

		  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		    int i = 0;
		    for (IntWritable val : values) {
		      i += val.get();
		    }
		    Put put = new Put(Bytes.toBytes(key.toString()));
		    put.addColumn(CF, COUNT, Bytes.toBytes(String.valueOf(i)));

		    context.write(null, put);
		  }
		}

}
