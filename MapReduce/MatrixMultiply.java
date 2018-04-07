import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiply {

	public static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int m = Integer.parseInt(conf.get("m"));
			int p = Integer.parseInt(conf.get("p"));
			String line = value.toString();
			String[] indexAndValue = line.split(",");
			Text outputKey = new Text();
			Text outputValue = new Text();
			if (indexAndValue[0].equals("M")) {
				for (int k = 0; k < p; k++) {
					outputKey.set(indexAndValue[1] + "," + k);
					outputValue.set(indexAndValue[0] + "," + indexAndValue[2] + "," + indexAndValue[3]);
					context.write(outputKey, outputValue);
				}
			} else {
				for (int i = 0; i < m; i++) {
					outputKey.set(i + "," + indexAndValue[2]);
					outputValue.set("N," + indexAndValue[1] + "," + indexAndValue[3]);
					context.write(outputKey, outputValue);
				}
			}
		}
	}
	
	public static class Reduce extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] value;
			HashMap<Integer, Float> hashMapA = new HashMap<Integer, Float>();
			HashMap<Integer, Float> hashMapB = new HashMap<Integer, Float>();
			for (Text val : values) {
				value = val.toString().split(",");
				if (value[0].equals("M")) {
					hashMapA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				} else {
					hashMapB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}
			}
			int n = Integer.parseInt(context.getConfiguration().get("n"));
			float result = 0.0f;
			float m_ij;
			float n_jk;
			for (int j = 0; j < n; j++) {
				m_ij = hashMapA.containsKey(j) ? hashMapA.get(j) : 0.0f;
				n_jk = hashMapB.containsKey(j) ? hashMapB.get(j) : 0.0f;
				result += m_ij * n_jk;
			}
			if (result != 0.0f) {
				context.write(null, new Text(key.toString() + "," + Float.toString(result)));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		conf.set("m", "500");
		conf.set("n", "500");
		conf.set("p", "500");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MatrixMultiply");
		job.setJarByClass(MatrixMultiply.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}