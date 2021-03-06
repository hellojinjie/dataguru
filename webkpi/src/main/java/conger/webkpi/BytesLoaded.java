package conger.webkpi;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class BytesLoaded {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: pageview <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "page view");
    job.setJarByClass(BytesLoaded.class);
    job.setMapperClass(PageViewMapper.class);
    job.setReducerClass(PageViewReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);           
  }

  public static class PageViewMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Pattern pattern = Pattern
        .compile("([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\")");

    private Text emptyValue = new Text();
    private Text one = new Text("" + 1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws java.io.IOException,
        InterruptedException {
      String line = value.toString();
      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        if (matcher.groupCount() >= 9) {
          String bytes = matcher.group(7);
          try {
            one.set(bytes);
            context.write(emptyValue, one);
          } catch (NumberFormatException e) {
          }
        }
      }
    }
  }

  public static class PageViewReducer extends Reducer<Text, Text, Text, Text> {

    private Text emptyText = new Text();
    private long totalView = 0;
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws java.io.IOException, InterruptedException {
      Iterator<Text> iter = values.iterator();
      while (iter.hasNext()) {
        Text t = iter.next();
        totalView += Integer.parseInt(t.toString());
      }
      context.write(emptyText, new Text("" + totalView));
      System.out.println(totalView);
    }
  }
}
