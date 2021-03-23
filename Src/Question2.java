// importing all the necessary headers
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Question2 {

	static class AvgPassengerMapper extends Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context) throws IOException,
		InterruptedException {
			// splitting the 1st row into columns
			String [] columns = value.toString().split(",");
			// handling the header from CSV, we want all the rows except the fist row which contains the column headers
			if(!columns[0].equals("VendorID"))
					{

				try {
					// getting the date in simple date format 	
					Date date=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(columns[1]);
					// passing the 4th column to the reducer as it contains trip distance
					context.write(new Text("Average_Total_Distance"), new FloatWritable(Float.parseFloat(columns[4])));
					// daykey is created which will have the day appended with text _Day_Total_Avg_Distance"
					Text daykey = new Text(new SimpleDateFormat("EEEE").format(date)+"_Day_Total_Avg_Distance");
					// sending the intermidiate key-value pair to reducer
					context.write(daykey,new FloatWritable(Float.parseFloat(columns[4])));
				} catch (ParseException e) { // handling the exceptions that may arise during parsing
					e.printStackTrace();
				}
					}
		}
	}

	static class AvgPassengerReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			  float sum = 0;
			  int count = 0;
			  for (FloatWritable value : values) {
				  // fetch all values associated with a key and add it to sum  variable
                sum += value.get();
                ++count; // count is increamented every time 
            }

		      context.write(key, new FloatWritable(sum / (float)count)); // type cast integer count to float

		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Question2");
		job.setJarByClass(Question2.class);
		job.setMapperClass(AvgPassengerMapper.class);
		job.setReducerClass(AvgPassengerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}