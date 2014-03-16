package conger.webkpi;

import java.net.URL;
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

public class Referrals {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: pageview <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "page view");
    job.setJarByClass(Referrals.class);
    job.setMapperClass(ReferralsMapper.class);
    job.setReducerClass(ReferralsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);           
  }

  public static class ReferralsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Pattern pattern = Pattern
        .compile("([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\")");

    private Text outKey = new Text();
    private Text one = new Text("1");

    @Override
    public void map(LongWritable key, Text value, Context context) throws java.io.IOException,
        InterruptedException {
      String line = value.toString();
      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        if (matcher.groupCount() >= 9) {
          String location = matcher.group(5);
          if (location.contains("ctp080113") || location.contains("popwin_js")) {
            return;
          }
          String referral = matcher.group(8);
          try {
            String url = referral.substring(1, referral.length() - 2);
            if ("-".equals(url)) {
              outKey.set("-");
            } else {
              URL url1 = new URL(url);
              String domain = url1.getHost();
              outKey.set(domain);
            }
            context.write(outKey, one);
          } catch (NumberFormatException e) {
          }
        }
      }
    }
  }

  public static class ReferralsReducer extends Reducer<Text, Text, Text, Text> {

    private Text outKey = new Text();
    private int totalView = 0;
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws java.io.IOException, InterruptedException {
      totalView = 0;
      Iterator<Text> iter = values.iterator();
      while (iter.hasNext()) {
        totalView++;
      }
      outKey.set(key);
      context.write(outKey, new Text("" + totalView));
      System.out.println(totalView);
    }
  }
}
