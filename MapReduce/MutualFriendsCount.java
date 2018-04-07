import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.*;

public class MutualFriendsCount 
{

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

	Text userNum = new Text();
	Text userFriends = new Text();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
	    String[] splitArray = value.toString().split("\\t");
	    String userID = splitArray[0];
	    if( splitArray.length == 1 ) {
	        return;
	    }
	    String[] others = splitArray[1].split(",");
	    for( String frnd : others )
	    {
	        if( userID.equals(frnd) )
	            continue;
	        
	        String userKey = (Integer.parseInt(userID) < Integer.parseInt(frnd) ) ? userID + "," + frnd : frnd + "," + userID;
	        String regex = "((\\b" + frnd + "[^\\w]+)|\\b,?" + frnd + "$)";
	        userFriends.set(splitArray[1].replaceAll(regex, ""));
	        userNum.set(userKey);
	        context.write(userNum, userFriends);
	    }
	}

}
	
public static class Reduce extends Reducer<Text,Text,Text,Text> {

private String findMatchingFriends( String frndList1, String frndList2 ) {

    if( frndList1 == null || frndList2 == null )
        return null;

    String[] friendsListArr1 = frndList1.split(",");
    String[] friendsListArr2 = frndList2.split(",");

    LinkedHashSet<String> set1 = new LinkedHashSet<String>();
    for( String user: friendsListArr1 ) {
        set1.add(user);
    }

    LinkedHashSet<String> set2 = new LinkedHashSet<String>();
    for( String user: friendsListArr2 ) {
        set2.add(user);
    }

    set1.retainAll(set2);
    return set1.toString().replaceAll("\\[|\\]","");
}

	public void reduce(Text key, Iterable<Text> values,Context context)
		throws IOException, InterruptedException 
		{
		    String[] friendsList = new String[2];
		    int i = 0;
		
		    for( Text value: values ) {
		        friendsList[i++] = value.toString();
		    }
		    String mutualFriends = findMatchingFriends(friendsList[0],friendsList[1]);
		    if( mutualFriends != null && mutualFriends.length() != 0 ) {
		        context.write(key, new Text( mutualFriends ) );
		    }
		}
}
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) 
		{
		    System.err.println("Usage: MutualFriends <FriendsFile> <output>");
		    System.exit(2);
		}
		
		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriendsCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

