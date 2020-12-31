import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RecommendGenre {

  
  public static class MovieWritable implements WritableComparable<MovieWritable>
    {

     private Text movieId, movieTitle;
     private DoubleWritable rating;

     //Default Constructor
     public MovieWritable() 
     {
      this.movieId = new Text();
      this.movieTitle = new Text();
      this.rating = new DoubleWritable();
     }

     //Custom Constructor
     public MovieWritable(DoubleWritable rate, Text id, Text title) 
     {
      this.movieId = id;
      this.movieTitle = title;
      this.rating = rate;
     }

     //Setter method to set the values of MovieWritable object
     public void set(DoubleWritable rate, Text id, Text title) 
     {
      this.movieId = id;
      this.movieTitle = title;
      this.rating = rate;
     }
     
     @Override
     //overriding default readFields method. 
     //It de-serializes the byte stream data
     public void readFields(DataInput in) throws IOException 
     {
      rating.readFields(in);
      movieTitle.readFields(in);
      movieId.readFields(in);
     }

     @Override
     //It serializes object data into byte stream data
     public void write(DataOutput out) throws IOException 
     {
      rating.write(out);
      movieTitle.write(out);
      movieId.write(out);
     }
     
     @Override
     public int compareTo(MovieWritable o) 
     {
      return rating.compareTo(o.rating);
     }

     @Override
     public boolean equals(Object o) 
     {
       if (o instanceof MovieWritable) 
       {
         MovieWritable other = (MovieWritable) o;
         return movieId.equals(other.movieId);
       }
       return false;
     }

     public Text getTitle()
     {
      return movieTitle;
     }

     public DoubleWritable getRating()
     {
      return rating;
     }
  }


  public static class MovieToGenreMapper
       extends Mapper<Object, Text, Text, MovieWritable>{

    private MovieWritable objWrite = new MovieWritable();
    private Text genre = new Text();
    private static final int MOVIE_ID = 0;
    private static final int MOVIE_TITLE = 1;
    private static final int GENRES = 2;
    private static final int RATING = 3;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
      String[] itr = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      String[] genres = new String[3];
      Text id = new Text();
      Text title = new Text();
      DoubleWritable rating = new DoubleWritable();

      int tokenNumber = 0;
      for (String s : itr) {
        if (tokenNumber == MOVIE_ID) {
          id.set(s);
        }
        if (tokenNumber == MOVIE_TITLE) {
          title.set(s);
        }
        if (tokenNumber == RATING) {
          rating.set(Double.parseDouble(s));
        }
        if (tokenNumber == GENRES) {
          genres = s.split("|");
        }

        tokenNumber++;
      }

      for (String s: genres) {
        genre.set(s);
        objWrite.set(rating, id, title);
        context.write(genre, objWrite);
      }
    }
  }

  public static class RecommendationGenReducer
       extends Reducer<Text,MovieWritable,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<MovieWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String recommendations = "";
      for (MovieWritable m : values) {
        String rating_with_title = m.getTitle().toString() + "\t" + m.getRating().toString();
        if (recommendations.equals("")) {
          recommendations = rating_with_title;
        } else {
          recommendations = recommendations + "|" + rating_with_title;
        }
      }
      result.set(recommendations);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Recommend Genre");
    job.setJarByClass(RecommendGenre.class);
    job.setMapperClass(MovieToGenreMapper.class);
    job.setReducerClass(RecommendationGenReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MovieWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

