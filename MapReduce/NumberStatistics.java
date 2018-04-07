import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NumberStatistics {
	 public static class Printer implements Writable{
	        private double sum;
	        private int count;
	        private double sumOfSquares;
	       
	        public Printer(){
	        }

	        public void write(DataOutput out) throws IOException{
	            out.writeDouble(sum);
	            out.writeInt(count);
	            out.writeDouble(sumOfSquares);
	        }

	        public void readFields(DataInput in) throws IOException{
	            sum = in.readDouble();
	            count = in.readInt();
	            sumOfSquares = in.readDouble();
	        }

	        public double getSum(){
	            return sum;
	        }

	        public int getCount(){
	            return count;
	        }
	       
	        public double getSumOfSquare(){
	            return sumOfSquares;
	        }

	        public void setSumAndCount(double sum, int count){
	            this.sum = sum;
	            this.count = count;
	        }
	       
	        public void setSumOfSquare(double squareSum){
	            this.sumOfSquares = squareSum;
	        }
	       	       	       
	        @Override
	        public String toString() {
	            // TODO Auto-generated method stub
	            return ""+sum+ "    " + sumOfSquares ;
	        }
	       
	    }
	   	   	   
	    public static class Map extends Mapper<LongWritable, Text, Text,Printer>{
	       
	        private Text word = new Text();
	        private Printer toPrint = new Printer();

	        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
	            String line = values.toString();
	           
	            toPrint.setSumAndCount(Double.parseDouble(line),1);
	           
	            word.set(new Text(""));
	            context.write(word,toPrint);

	        }

	    }
	   	   	   	   
	    public static class Combine extends Reducer<Text,Printer,Text,Printer> {
	        private Printer value = new Printer();
	       
	        public void reduce(Text key, Iterable<Printer> values,Context context) throws IOException, InterruptedException {
	           	           
	            double sum = 0;
	            int count = 0;
	            double squareSum = 0;
	           
	            for (Printer pair:values){
	                sum += pair.getSum();
	                count += pair.getCount();
	               
	                squareSum+=Math.pow(pair.getSum(),2);
	                       
	            }
	           	           
	            value.setSumAndCount(sum, count);
	            value.setSumOfSquare(squareSum);
	           
	            context.write(key, value);	           	           
	    }

	    }   
	   
	    public static class Reduce extends Reducer<Text,Printer,Text,Printer> {
	        private Printer val = new Printer();
	       
	        public void reduce(Text key, Iterable<Printer> values,Context context) throws IOException, InterruptedException {
	            double sum = 0; 
	            int count = 0;
	            double squareSum = 0;   
	           
	            for (Printer pair:values){
	                sum += pair.getSum();
	                count += pair.getCount();
	               
	                squareSum += pair.getSumOfSquare();
	               
	            }
	           
	            double average = sum/count;
	            double variance = (squareSum/count) - Math.pow(average,2);
	           	           
	            System.out.println("Average: "+average);
	            System.out.println("Variance: "+ variance);
	            val.setSumAndCount(average, count);
	            val.setSumOfSquare(variance);
	           
	            context.write(key,val);
	        }
	    }

	      public static void main(String[] args) throws Exception {
	     
	      Configuration conf = new Configuration();
	      String[] arguments = new GenericOptionsParser(conf, args).getRemainingArgs();
	     
	      if (arguments.length != 2) {
	      System.err.println("Usage: WordCount <in> <out>");
	      System.exit(2);
	      }
	     
	      Job job = new Job(conf, "MeanAndVariance");
	      job.setJarByClass(NumberStatistics.class);
	      job.setMapperClass(Map.class);
	      job.setCombinerClass(Combine.class);
	      job.setReducerClass(Reduce.class);
	      job.setOutputKeyClass(Text.class);	     
	      job.setOutputValueClass(Printer.class);	     
	      FileInputFormat.addInputPath(job, new Path(arguments[0]));
	      FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
	      System.exit(job.waitForCompletion(true) ? 0 : 1);
	      }

}
