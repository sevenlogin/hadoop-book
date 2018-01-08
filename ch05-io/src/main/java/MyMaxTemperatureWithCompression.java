import oldapi.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Created by root on 17-12-20.
 */
public class MyMaxTemperatureWithCompression {
    public static void main(String[] args) throws IOException {
        args = new String[2];
        args[0] = "file://" + System.getProperty("user.dir") + "/input/ncdc/sample.txt";
        args[1] = "file://" + System.getProperty("user.dir") + "/my-hadooptest-output/ch05/compress";

        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }


        JobConf job = new JobConf(oldapi.MaxTemperature.class);
        job.setJobName("Max temperature");


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //mapper reducer combiner
        job.setMapperClass(MyMaxTemperatureMapper.class);
        job.setReducerClass(MyMaxTemperatureReducer.class);

        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        JobClient.runJob(job);
    }
}
