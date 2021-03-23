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


public class Question5 {

static class AverageTripDistancePerDayMapper extends Mapper<Object, Text, Text, FloatWritable> {
	private Text word = new Text();
	public void map(Object key, Text value, Context context) throws IOException,
	InterruptedException {
			word.set(value);
			String nextLine =word.toString();
			// splitting the rows into seperate columns
			String [] columns=nextLine.split(",");
			
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
								Text columnkey=new Text("WEEK END"+"_TimeDur_"+date.getHours()+"_TripDistance");
								context.write(columnkey, new FloatWritable(Float.parseFloat(columns[4])));
								}
								else
								{	
							//  if the day is any other day then this is weekday and the key is appended with WEEK DAY text 
								Text columnkey=new Text("WEEK DAY "+"_TimeDur_"+date.getHours()+"_TripDistance");
								context.write(columnkey, new FloatWritable(Float.parseFloat(columns[4])));
								}
							}
						} catch (ParseException e) { // handling the exceptions that may arise during parsing
							e.printStackTrace();
						}	
					
		}
}


static class AverageTripDistancePerDayReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException {
		float sum = 0;
		
            int count = 0;
            for (final FloatWritable value : values) {
				// fetch all values associated with a key and add it to sum  variable
                sum += value.get();
                ++count;
				// count is increamented every time to keep the count of total entries
            }
             context.write(key, new FloatWritable(sum /(float)count)); //average is calculated
        }
    }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Question5");
    job.setJarByClass(Question5.class);
    job.setMapperClass(AverageTripDistancePerDayMapper.class);
    job.setReducerClass(AverageTripDistancePerDayReducer.class);
	job.setCombinerClass(AverageTripDistancePerDayReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

