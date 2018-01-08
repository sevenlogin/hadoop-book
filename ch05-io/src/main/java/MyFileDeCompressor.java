import org.apache.commons.httpclient.URI;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * Created by root on 17-12-19.
 */
public class MyFileDeCompressor {
    public static void main(String[] args) throws Exception {
        //MyFileDeCompressor.class.getClassLoader().getResourceAsStream()
        args = new String[2];
//        args[0] = "file://" + System.getProperty("user.dir") + "/input/ncdc/all/1901.gz";
        args[0] = "file:///opt/1.txt.gz";
        System.out.println(args[0]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(java.net.URI.create(args[0]), conf);

        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = codecFactory.getCodec(new Path(args[0]));
        if (codec == null) {
            System.err.println("No codec found for " + args[0]);
            System.exit(-1);
        }


        CompressionInputStream in = codec.createInputStream(fs.open(new Path(args[0])));

        String outputUri = CompressionCodecFactory.removeSuffix(args[0], codec.getDefaultExtension());
        FSDataOutputStream output = fs.create(new Path(outputUri));

        IOUtils.copyLarge(in, output, new byte[4096]);
        IOUtils.closeQuietly(in);
        IOUtils.closeQuietly(output);
    }
}
