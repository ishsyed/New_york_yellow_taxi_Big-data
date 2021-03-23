// Importing the headers
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
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


public class Question4 {

	static class AvgPassengerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException,
		InterruptedException {
			word.set(value);
			String [] columns = value.toString().split(",");
			// splitting each row into seperate columns
			  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

				try {
				if (!columns[0].equals("VendorID"))
					// handling the header from CSV, we want all the rows except the fist row which contains the column headers
							{
								Date date=df.parse(columns[1]);
								// getting the date in simple date format 
								if(date.getDay()==0 || date.getDay()==6) 
								{
								// checking if the day is sunday or saturday.If it is then this is weekend and the key is appended with WEEK END text 
								Text columnkey=new Text("WEEK END"+"_TimeDur_"+date.getHours()+"_Avg_Passanger");
								context.write(columnkey, new IntWritable(Integer.parseInt(columns[3])));
								}
								else
									// checking if the day is any other day then this is weekday and the key is appended with WEEK DAY text 
								{	
								Text columnkey=new Text("WEEK DAY "+"_TimeDur_"+date.getHours()+"_Avg_Passanger");
								context.write(columnkey, new IntWritable(Integer.parseInt(columns[3])));
								}
							}
				} catch (ParseException e) {
					e.printStackTrace(); // handling the exceptions that may arise during parsing
				}
		}
	}

	static class AvgPassengerReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			 int sum = 0;
            int count = 0;
            for (IntWritable value : values) {
				// fetch all values associated with a key and add it to sum  variable
                sum += value.get();
                ++count;
				// count is increamented every time to keep the count of total entries  
            }
             context.write(key, new FloatWritable((float)sum /(float)count)); //average is calculated 

		}
	}

public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Question4");
		job.setJarByClass(Question4.class);
		job.setMapperClass(AvgPassengerMapper.class);
		job.setReducerClass(AvgPassengerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}