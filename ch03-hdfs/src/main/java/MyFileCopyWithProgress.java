import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by root on 17-12-18.
 */
public class MyFileCopyWithProgress {
    public static void main(String[] args) throws IOException {
        args = new String[2];
        args[0] = "file://" + System.getProperty("user.dir") + "/input/ncdc/sample.txt";
        //args[1] = "file://" + System.getProperty("user.dir") + "/my-hadooptest-output/ch03/sample.txt";
        args[1] = "hdfs://hadoop-single:9000/my-hadooptest-output/ch03/sample.txt";

        FileSystem fs = FileSystem.get(URI.create(args[1]), new Configuration());
        FSDataOutputStream out = fs.create(new Path(args[1]), new Progressable() {
            @Override
            public void progress() {
                //本例该进度条不用打印,hadoop只有写入到hdfs文件系统中才会调用progress方法
                System.out.print(".");
            }
        });
        IOUtils.copyLarge(new FileInputStream(URI.create(args[0]).getPath()), out, new byte[4096]);
        IOUtils.closeQuietly(out);
        //org.apache.hadoop.io.IOUtils.copyBytes(new FileInputStream(URI.create(args[0]).getPath()), out, 4096, true);
    }
}
