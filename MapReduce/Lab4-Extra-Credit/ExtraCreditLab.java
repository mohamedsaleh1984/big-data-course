import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExtraCreditLab extends Configured implements Tool {

	public static class AverageTemperatureMapper extends
			Mapper<LongWritable, Text, StationTempPair, IntWritable> {
		@Override
		protected void map(
				LongWritable key,
				Text lineInput,
				Mapper<LongWritable, Text, StationTempPair, IntWritable>.Context context)
				throws IOException, InterruptedException {

	//		System.out.println("Start Mapping....");

			String strLine = lineInput.toString();

			// Extract Station Id
			String stationIdLP = strLine.substring(4, 10);
			String stationIdRP = strLine.substring(10, 15);
			String stationId = stationIdLP + "-" + stationIdRP;
			
		//	System.out.println("Station Id " + stationId);
			
			// Extract Year
			String strYear = strLine.substring(15, 19);
			int iYear = Integer.parseInt(strYear);
			// Extract Temperature
			float tempVal = Float.valueOf(strLine.substring(87, 92));

			// Construct Pair Key/Value....
			StationTempPair stp = new StationTempPair();
			stp.setStationId(stationId);
			stp.setTemperature(tempVal);
	
			context.write(stp, new IntWritable(iYear));
			
		}
	}

	public static class AverageTemperatureReducer extends
			Reducer<StationTempPair, IntWritable, Text, Text> {
		@Override
		protected void reduce(StationTempPair redInput, Iterable<IntWritable> yearsList,
				Reducer<StationTempPair, IntWritable, Text, Text>.Context contxt)
				throws IOException, InterruptedException {

			
			System.out.println("RED_INPUT " + redInput.getStationId());
			
			Iterator<IntWritable>  yearsPerPair = yearsList.iterator();
			
			while(yearsPerPair.hasNext()){
				
				IntWritable year = yearsPerPair.next();
				
				float fTemp = redInput.getTemperature();
				
				String tmp = fTemp +"\t" + year.get();
				
				System.out.println("Output " + tmp);
				
				contxt.write(new Text(redInput.getStationId()), new Text(tmp));
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "StationTemp");
		job.setJarByClass(ExtraCreditLab.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);

		// Output Paramaters
		job.setMapOutputKeyClass(StationTempPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Delete Output Folder Recursively If exists.

		File f = new File(args[1]);
		if (f.exists() && f.isDirectory()) {
			FileUtils.deleteDirectory(f);
		}

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new ExtraCreditLab(), args);

		System.exit(res);
	}

}
