// Importing the headers
import java.io.IOException;
import java.util.StringTokenizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Question3 {

static class PaymentTypeMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException,
	InterruptedException {
		
			word.set(value);
			String nextLine =word.toString();
			// splitting the rows into each seperate columns
			String [] columns=nextLine.split(",");
			try {
				// handling the header from CSV, we want all the rows except the fist row which contains the column headers
							if (!columns[0].equals("VendorID"))
							{
			
								final IntWritable one = new IntWritable(1);
								// the value passed is just an iterator which is always 1 and the key is payment type from column 9
								context.write(new Text(columns[9]), one);
								
							}
				} catch (Exception e) { // handling the exceptions that may arise during parsing
							e.printStackTrace();
						}			
				

			
		}
}

static class PaymentTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
	public void reduce( final Text key, final Iterable<IntWritable> values, Context context) throws
	IOException, InterruptedException { 
		int sum = 0;
			for (final IntWritable value : values) {
				 // fetch all values associated with a key and add it to sum  variable 
                sum += value.get();
            }
				context.write(key, new IntWritable(sum));
	}
}

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Question3");
    job.setJarByClass(Question3.class);
    job.setMapperClass(PaymentTypeMapper.class);
    job.setReducerClass(PaymentTypeReducer.class);
	job.setCombinerClass(PaymentTypeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

