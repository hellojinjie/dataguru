package conger.webkpi;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.UserAgentType;
import net.sf.uadetector.service.UADetectorServiceFactory;

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

public class IndependentIP {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: independentip <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "independent ip");
    job.setJarByClass(IndependentIP.class);
    job.setMapperClass(IndependentIPMapper.class);
    job.setReducerClass(IndependentIPReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);           
  }

  public static class IndependentIPMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Pattern pattern = Pattern
        .compile("([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\")");

    private UserAgentStringParser uaParser = UADetectorServiceFactory.getCachingAndUpdatingParser();

    private Text ip = new Text();
    private Text emptyValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws java.io.IOException,
        InterruptedException {
      String line = value.toString();
      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        if (matcher.groupCount() >= 9) {
          String ua = matcher.group(9);
          ReadableUserAgent userAgent = uaParser.parse(ua);
          if (userAgent.getType().compareTo(UserAgentType.ROBOT) == 0) {
            /* ignore robot */
            return;
          }
        }
        ip.set(matcher.group(1));
        context.write(emptyValue, ip);
      }
    }
  }

  public static class IndependentIPReducer extends Reducer<Text, Text, Text, Text> {

    private Text emptyText = new Text();
    private Set<String> ips = new HashSet<String>();
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws java.io.IOException, InterruptedException {
      Iterator<Text> iter = values.iterator();
      while (iter.hasNext()) {
        Text t = iter.next();
        ips.add(t.toString());
      }
      context.write(emptyText, new Text("" + ips.size()));
      System.out.println(ips.size());
    }
  }
}
