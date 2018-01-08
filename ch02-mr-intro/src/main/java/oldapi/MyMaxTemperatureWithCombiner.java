package oldapi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;


public class MyMaxTemperatureWithCombiner {

    public static void main(String[] args) throws IOException {
        args = new String[2];
        args[0] = "file://" + System.getProperty("user.dir") + "/input/ncdc/sample.txt";
        args[1] = "file://" + System.getProperty("user.dir") + "/my-hadooptest-output";

        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        JobConf job = new JobConf(MyMaxTemperature.class);
        job.setJobName("MyMaxTemperature-test");

        //input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //mapper reducer combiner config
        job.setMapperClass(MyMaxTemperatureMapper.class);
        job.setReducerClass(MyMaxTemperatureReducer.class);
        job.setCombinerClass(MyMaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        JobClient.runJob(job);
    }
}
