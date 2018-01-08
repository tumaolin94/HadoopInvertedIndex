package code.maolin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class InvertIndex extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(InvertIndex.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new InvertIndex(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable two = new IntWritable(2);
    private Text word = new Text();
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    private static final Pattern DOCUMENT_BOUNDARY = Pattern.compile("\\t");
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      System.out.println("test: "+line);
      String[] test = DOCUMENT_BOUNDARY.split(line);
//      System.out.println("Document id: "+ test[0]);
//      System.out.println("Document text: "+ test[1]);
      String documentId = test[0];
      Text docid = new Text(documentId);
      Text currentWord = new Text();
      for (String word : WORD_BOUNDARY.split(test[1])) {
        if (word.isEmpty()) {
            continue;
        }
            currentWord = new Text(word);
            context.write(currentWord,docid);
        }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
	  HashMap< String, Integer > map = new HashMap <> ();
    @Override
    public void reduce(Text word, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (Text value : values) {
    	  String tmpWord = value.toString();
        if(map.containsKey(tmpWord)){
        	map.put(tmpWord,map.get(tmpWord)+1);
        }else{
        	map.put(tmpWord, 1);
        }
      }
      
      StringBuilder sb = new StringBuilder();
      for(String docId: map.keySet()){
    	  sb.append(docId);
    	  sb.append(": ");
    	  sb.append(map.get(docId)+"\t");
      }
      context.write(word, new Text(sb.toString()));
    }
  }
}
