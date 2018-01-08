import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Created by root on 17-12-18.
 */
public class MyFileSystemDoubleCat {
    public static void main(String[] args) throws IOException {
        args = new String[] {"hdfs://hadoop-single:9000/user/root/input/core-site.xml"};

        FileSystem fs = FileSystem.get(URI.create(args[0]), new Configuration());

        FSDataInputStream in = fs.open(new Path(args[0]));
        IOUtils.copyBytes(in, System.out, 4096, false);
        in.seek(0);
        org.apache.commons.io.IOUtils.copyLarge(in, System.out, new byte[4096]);
    }
}
