import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroGenericStationMaxTempYear extends Configured implements Tool
{
	private static Schema SCHEMA;
	private static HashMap<Integer, AvroKey<GenericRecord>> checker = new HashMap<Integer, AvroKey<GenericRecord>>();
	
	
	public static class AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>
	{
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord record = new GenericData.Record(SCHEMA);
		AvroKey<GenericRecord> avroKey = new AvroKey<GenericRecord>(record);
		//Keep Track of Temp Changes
		HashMap<Integer,Float> YearTempTracker = new HashMap<Integer, Float>();
		HashMap<Integer,String> YearStationTracker = new HashMap<Integer, String>();
		
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			utils.parse(value.toString());

			if (utils.isValidTemperature())
			{
				 
				String incomingStationId = utils.getStationId();
				float incomingTemp = utils.getAirTemperature();
				int incomingYear = utils.getYearInt();
				
				if(YearTempTracker.containsValue(incomingYear)){
					if(incomingTemp > YearTempTracker.get(incomingYear)){
						YearTempTracker.put (incomingYear, incomingTemp);
						YearStationTracker.put(incomingYear,incomingStationId);
					}
				}else{
					YearTempTracker.put(incomingYear, incomingTemp);
					YearStationTracker.put(incomingYear,incomingStationId);
				}
			}
		}
		
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for(Integer yearKey : YearTempTracker.keySet()){
				record.put("StationId", YearStationTracker.get(yearKey));
				record.put("MaxTemp", YearTempTracker.get(yearKey));
				record.put("Year", yearKey);				
				context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
			}	 
		}
	}
	

	public static class AvroReducer extends Reducer<AvroKey<GenericRecord>, FloatWritable, AvroKey<GenericRecord>, NullWritable>
	{
		@Override
		protected void setup(
				Reducer<AvroKey<GenericRecord>, FloatWritable, AvroKey<GenericRecord>, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
		}
		
		@Override
		protected void reduce(AvroKey<GenericRecord> key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException
		{		 
			context.write(key, NullWritable.get());
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(AvroGenericStationMaxTempYear.class);
		job.setJobName("Avro Station-Temp-Year");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String schemaFile = args[2];

		//Load the schema file
		SCHEMA = new Schema.Parser().parse(new File(schemaFile));

		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);
		
		
		job.setMapOutputValueClass(NullWritable.class);
		
		AvroJob.setMapOutputKeySchema(job, SCHEMA);  
		AvroJob.setOutputKeySchema(job,SCHEMA); 
		
		job.setOutputFormatClass(AvroKeyOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path("output"), true);
		int res = ToolRunner.run(conf, new AvroGenericStationMaxTempYear(), args);		
		System.exit(res);
	}
}