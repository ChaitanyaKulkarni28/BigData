import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
 public class minMaxMedian {
     
	 public static class getterSetter implements Writable
	   {
	     Integer minimum;
	     Integer maximum;
	     Double median;
	   
	     public getterSetter()
	     {
	       this.minimum = Integer.valueOf(0);
	       this.maximum = Integer.valueOf(0);
	       this.median = Double.valueOf(0.0D);
	     }
	   
	     public void write(DataOutput out) throws IOException {
	       out.writeInt(this.minimum.intValue());
	       out.writeInt(this.maximum.intValue());
	       out.writeDouble(this.median.doubleValue());
	     }
	   
	     public void readFields(DataInput in) throws IOException {
	       this.minimum = new Integer(in.readInt());
	       this.maximum = new Integer(in.readInt());
	       this.median = new Double(in.readDouble());
	     }
	   
	     public String toString()
	     {
	       return "Minimum = " + this.minimum + "\tMaximum = " + this.maximum + "\tMedian = " + this.median;
	     }
	   
	     public Integer getMinimum() {
	       return this.minimum;
	     }
	   
	     public Integer getMaximum() {
	       return this.maximum;
	     }
	   
	     public Double getMedian() {
	       return this.median;
	     }
	   
	     public void setMinimum(Integer minimum) {
	       this.minimum = minimum;
	     }
	   
	     public void setMaximum(Integer maximum) {
	       this.maximum = maximum;
	     }
	   
	     public void setMedian(Double median) {
	       this.median = median;
	     }
	   }
	 
       public static class Map extends Mapper<LongWritable, Text, Text, getterSetter>
       {
         private Integer Minimum;
         private Integer Maximum;
         private Double Median;
	     private getterSetter output = new getterSetter();
     
         public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, getterSetter>.Context context) throws IOException, InterruptedException {
         	String number = value.toString();
     
          	Minimum = Integer.valueOf(Integer.parseInt(number));
         	Maximum = Integer.valueOf(Integer.parseInt(number));
         	Median = Double.valueOf(Double.parseDouble(number));
     
         	output.setMinimum(Minimum);
         	output.setMaximum(Maximum);
         	output.setMedian(Median);
   
         context.write(new Text("Minimum/Maximum/Median"), output);
    }
 }

 public static class Reduce extends Reducer<Text, getterSetter, NullWritable, Text> {
 	      private getterSetter result = new getterSetter();
    	  List<Double> listOfNumbers = new ArrayList();
     
          public void reduce(Text key, Iterable<getterSetter> values, Reducer<Text, getterSetter, NullWritable, Text>.Context context)
           throws IOException, InterruptedException
          {
        	  result.setMinimum(null);
        	  result.setMaximum(null);
    	      result.setMedian(null);
     
         for (getterSetter val : values) {
           Integer Minimum = val.getMinimum();
           Integer Maximum = val.getMaximum();
           Double Median = val.getMedian();
     
           listOfNumbers.add(Median);
     
           if ((result.getMinimum() == null) || (Minimum.compareTo(result.getMinimum()) < 0)) {
             	result.setMinimum(Minimum);
             }
           if ((result.getMaximum() == null) || (Maximum.compareTo(result.getMaximum()) > 0)) {
             	result.setMaximum(Maximum);
             }
           }
     
         Collections.sort(listOfNumbers);
         int sizeOfList = listOfNumbers.size();
     
         if (sizeOfList % 2 == 0) {
           int half = sizeOfList / 2;
     
           result.setMedian(((listOfNumbers.get(half - 1)) + (listOfNumbers.get(half)))/ 2.0D);
           } else {
           int half = (sizeOfList + 1) / 2;
           this.result.setMedian(listOfNumbers.get(half - 1));
           }
         context.write(NullWritable.get(), new Text(result.toString()));
         }
       }


   public static void main(String[] args) throws Exception {
     Configuration conf = new Configuration();
     String[] arguments = new GenericOptionsParser(conf, args).getRemainingArgs();
     
     if (arguments.length != 2) {
       System.err.println("Usage: <inputFile> <outputFile>");
       System.exit(2);
     }
 
       Job job = Job.getInstance(conf);
       job.setJobName("MinMaxMedian");
       job.setJarByClass(minMaxMedian.class);
       job.setMapperClass(minMaxMedian.Map.class);
       job.setReducerClass(minMaxMedian.Reduce.class);
     
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(getterSetter.class);
     
       job.setOutputKeyClass(NullWritable.class);
       job.setOutputValueClass(Text.class);
     
       FileInputFormat.addInputPath(job, new Path(arguments[0]));
       FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
     
       System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
 }
