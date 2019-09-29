package mutualfriends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {
	public static class Map
    extends Mapper<LongWritable, Text, Text, Text>{

private Text user = new Text();
private Text friends = new Text(); // type of output key

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] userFriends = value.toString().split("\\t");
    if(userFriends.length < 2) return;
    String userId = userFriends[0];
    String[] friendsList = userFriends[1].split(",");
    for (String friend : friendsList) {
    	if(userId.equals(friend)) continue;
    	String userKey = (userId.compareTo(friend) < 0) ? (userId + "," + friend) : (friend + "," + userId);
    	String regex = "((\\b" + friend + "[^\\w]+)|\\b,?" + friend +"$)";
    	String friendarray = userFriends[1].replaceAll(regex , "");
    	friends.set(friendarray);
    	user.set(userKey);
    	context.write(user, friends);
    }
}
}

public static class Reduce
    extends Reducer<Text,Text,Text,Text> {

	private String matchingFriends(String list1, String list2) {
		if(list1 == null || list2 == null) return null;
		String[] flist = list1.split(",");
		String[] slist = list2.split(",");
		LinkedHashSet<String> set1 = new LinkedHashSet<>();
		LinkedHashSet<String> set2 = new LinkedHashSet<>();
		Collections.addAll(set1, flist);
		Collections.addAll(set2, slist);
		set1.retainAll(set2);
		return set1.toString().replaceAll("\\[|\\]", "");
	}

public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	ArrayList<String> set = new ArrayList<>();
	for(Text value: values) {
		 set.add(value.toString());
	}
	if(set.size() == 1) set.add("");
	String mutualFriends = matchingFriends(set.get(0), set.get(1));
	if(mutualFriends != null && mutualFriends.length() != 0) {
		context.write(key, new Text(mutualFriends));
	}
}
}


// Driver program
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
// get all args
if (otherArgs.length != 2) {
    System.err.println("Usage: <in> <out>");
    System.exit(2);
}

// create a job with name "mutualfriends"
Job job = new Job(conf, "mutualfriends");
job.setJarByClass(MutualFriends.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);

// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

// set output key type
job.setOutputKeyClass(Text.class);
// set output value type
job.setOutputValueClass(Text.class);
//set the HDFS path of the input data
FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
// set the HDFS path for the output
FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//Wait till job completion
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
