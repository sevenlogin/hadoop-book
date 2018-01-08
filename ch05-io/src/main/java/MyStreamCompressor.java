// cc StreamCompressor A program to compress data read from standard input and write it to standard output
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

// vv StreamCompressor
public class MyStreamCompressor {

  public static void main(String[] args) throws Exception {
    GzipCodec codec = new GzipCodec();
    CompressionOutputStream out = codec.createOutputStream(System.out);
    IOUtils.copyBytes(System.in, out, 4096, false);
    out.flush();
  }
}
// ^^ StreamCompressor
