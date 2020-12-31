import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class HDFSReaderWriter {

    public static void main(String args[]) throws java.io.IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

		Path path = new Path("hdfs://localhost:54310/user/hadoop/test");
		FileSystem fs = path.getFileSystem(conf);

		FSDataInputStream input = fs.open(path);
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String line = null;
		while ((line=reader.readLine())!=null) {
			System.out.print(line);
		}
		input.close();

		FSDataOutputStream appendFile = fs.append(path);
		appendFile.writeChars("\nHello HDFS Guys \n");
		appendFile.close();

		fs.close();
    }
}

