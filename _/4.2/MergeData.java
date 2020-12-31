import java.util.*;  

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class MergeData {

  public static class Movie {
    String movieId = "";
    String movieTitle = "";
    double rating = 0.0;
    String genres = "";

    public Movie(String newMovieId, String newMovieTitle, double newRating, String newGenres) {
        movieId = newMovieId;
        movieTitle = newMovieTitle;
        rating = newRating;
        genres = newGenres;
    }
  }
 
  public static void main(String[] args) {
    HashMap<String, Movie> movies = new HashMap<String, Movie>();
    File ratingFile = new File("/home/hadoop/avg_rating.tsv");
    File movieFile = new File("/home/hadoop/movies.csv");
    File mergedFile = new File("/home/hadoop/movies_with_ratings.csv");
    FileInputStream fis = null;
    BufferedInputStream bis = null;
    DataInputStream dis = null;
 
    try {
      fis = new FileInputStream(ratingFile);
      bis = new BufferedInputStream(fis);
      dis = new DataInputStream(bis);
      while (dis.available() != 0) {
        String[] movieRating = dis.readLine().split("\t");
        movies.put(movieRating[0], new Movie(movieRating[0], "", Double.parseDouble(movieRating[1]), ""));
      } 
      fis.close();
      bis.close();
      dis.close();
 
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      fis = new FileInputStream(movieFile);
      bis = new BufferedInputStream(fis);
      dis = new DataInputStream(bis);
      boolean firstLine = true;
      while (dis.available() != 0) {
        if (firstLine) {
          firstLine = false;
          dis.readLine();
          continue;
        }
        String[] movieInfo = dis.readLine().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        Movie movie = movies.get(movieInfo[0]);
        if (movie == null) {
          movie = new Movie(movieInfo[0], "", 0.0, "");
        }
        movie.movieTitle = movieInfo[1];
        movie.genres = movieInfo[2];
        movies.put(movieInfo[0], movie);
      }
      fis.close();
      bis.close();
      dis.close();
 
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println(movies.size());

    try {
      FileOutputStream fos = new FileOutputStream(mergedFile);
 
      OutputStreamWriter osw = new OutputStreamWriter(fos);
     
      for (Map.Entry m:movies.entrySet()) {
        Movie movie = (Movie)m.getValue();
        String csvLine = movie.movieId + "," + movie.movieTitle + "," + movie.genres + "," + String.valueOf(movie.rating) + "\n";
        osw.write(csvLine);
      }

      osw.close();
 
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    

  }
}
