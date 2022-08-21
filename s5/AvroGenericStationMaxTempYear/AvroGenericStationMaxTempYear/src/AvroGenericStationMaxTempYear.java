import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			utils.parse(value.toString());

			if (utils.isValidTemperature())
			{
				String incomingStationId = utils.getStationId();
				float incomingTemp = utils.getAirTemperature();
				int incomingYear = utils.getYearInt();
				
				record.put("StationId",incomingStationId);
				record.put("MaxTemp", incomingTemp);
				record.put("Year",incomingYear);
				
				context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
			}
		}
	}
	
	public static class AvroReducer extends Reducer<AvroKey<GenericRecord>, FloatWritable, AvroKey<GenericRecord>, NullWritable>
	{
		private HashSet<Integer> mySet = new HashSet<Integer>(); 
		
		@Override
		protected void reduce(AvroKey<GenericRecord> key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException
		{
			int yearValue = (int) key.datum().get(0);
			if(!mySet.contains(yearValue)){
				mySet.add(yearValue);
				context.write(key, NullWritable.get());
			}
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
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		AvroJob.setMapOutputKeySchema(job, SCHEMA);  
		AvroJob.setOutputKeySchema(job,SCHEMA); 		
		

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