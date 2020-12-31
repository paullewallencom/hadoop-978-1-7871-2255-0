import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgRating {

  public static class CSVSplitMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    private DoubleWritable rating = new DoubleWritable(0.0);
    private Text word = new Text();
    private static final int USER_ID = 0;
    private static final int MOVIE_ID = 1;
    private static final int RATING = 2;
    private static final int TIMESTAMP = 3;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] itr = value.toString().split(",");
      int tokenNumber = 0;
      for (String s : itr) {
        if (tokenNumber == MOVIE_ID) {
          word.set(s);
        }
        if (tokenNumber == RATING) {
          rating.set(Double.parseDouble(s));
        }
        tokenNumber++;
      }
      context.write(word, rating);
    }
  }

  public static class DoubleAVGReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      double count = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
        count += 1.0;
      }
      result.set(sum/count);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Avg Rating");
    job.setJarByClass(AvgRating.class);
    job.setMapperClass(CSVSplitMapper.class);
    job.setReducerClass(DoubleAVGReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

