package mutualfriends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends10 {
	public static class Map1
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
    	friends.set(userFriends[1]);
    	user.set(userKey);
    	context.write(user, friends);
    }
}
}

public static class Reduce1
    extends Reducer<Text,Text,Text,Text> {

	private int matchingFriends(String list1, String list2) {
		if(list1 == null || list2 == null) return 0;
		String[] flist = list1.split(",");
		String[] slist = list2.split(",");
		LinkedHashSet<String> set1 = new LinkedHashSet<>();
		LinkedHashSet<String> set2 = new LinkedHashSet<>();
		Collections.addAll(set1, flist);
		Collections.addAll(set2, slist);
		set1.retainAll(set2);
		return set1.size();
	}

public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	ArrayList<String> set = new ArrayList<>();
	for(Text value: values) {
		 set.add(value.toString());
	}
	if(set.size() == 1) set.add("");
	int mutualFriends = matchingFriends(set.get(0), set.get(1));
	context.write(key, new Text(Integer.toString(mutualFriends)));
}
}

public static class Map2
extends Mapper<LongWritable, Text, Text, Text>{

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	context.write(new Text(""), value);
}
}

public static class Reduce2
extends Reducer<Text,Text,Text,IntWritable> {

public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
HashMap<String, Integer> map = new HashMap<>();
for(Text value: values) {
	 String[] list = value.toString().split("\\t");
	 if(list.length == 2) {
		 map.put(list[0], Integer.parseInt(list[1]));
	 }
}
TreeMap<String, Integer> sortedmap = new TreeMap<>(new Comparator<String>() {
	@Override
	public int compare(String s1, String s2) {
		return map.get(s2).compareTo(map.get(s1));
	}
});
sortedmap.putAll(map);
int count = 1;
for(String k: sortedmap.keySet()) {
	if(count <= 10) {
		context.write(new Text(k), new IntWritable(sortedmap.get(k)));
	}else break;
	count++;
}
}
}

// Driver program
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
// get all args
if (otherArgs.length != 3) {
    System.err.println("Usage: MutualFriends <in> <out>");
    System.exit(2);
}

// create a job with name "mutualfriends"
Job job = new Job(conf, "mutualfriends");
job.setJarByClass(MutualFriends10.class);
job.setMapperClass(Map1.class);
job.setReducerClass(Reduce1.class);

// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
// set output key type
job.setOutputKeyClass(Text.class);
// set output value type
job.setOutputValueClass(Text.class);
//set the HDFS path of the input data
FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
// set the HDFS path for the output
FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
//Wait till job completion
if(job.waitForCompletion(true)) {

conf = new Configuration();
// get all args
if (otherArgs.length != 3) {
    System.err.println("Usage: <in> <out> <temp>");
    System.exit(2);
}

// create a job with name "mutualfriends"
job = new Job(conf, "mutualfriends10");
job.setJarByClass(MutualFriends10.class);
job.setMapperClass(Map2.class);
job.setReducerClass(Reduce2.class);

job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
job.setInputFormatClass(TextInputFormat.class);
// set output key type
job.setOutputKeyClass(Text.class);
// set output value type
job.setOutputValueClass(IntWritable.class);
//set the HDFS path of the input data
FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
// set the HDFS path for the output
FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//Wait till job completion
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
}
