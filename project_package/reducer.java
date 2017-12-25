package sentiment_analysis_package;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {

	public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
		
		int number=0;
		float sum = 0;
		for (FloatWritable value : values) {
			sum += value.get();
			number++;
		}
		
		sum=(sum/number);
		
		String mykey = key.toString().toUpperCase();
		
		context.write(new Text(mykey), new FloatWritable(sum)); 
	}
}