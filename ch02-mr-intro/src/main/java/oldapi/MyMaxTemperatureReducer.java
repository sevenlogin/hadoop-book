package oldapi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


public class MyMaxTemperatureReducer implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int maxTemperature = 0;
        while (values.hasNext()) {
            /*IntWritable temperature = values.next();
            if (temperature.get() > maxTemperature) {
                maxTemperature = temperature.get();
            }*/
            maxTemperature = Math.max(maxTemperature, values.next().get());
        }
        output.collect(key, new IntWritable(maxTemperature));
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf job) {

    }
}
