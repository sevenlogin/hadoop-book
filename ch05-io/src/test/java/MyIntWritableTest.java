import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.StringUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MyIntWritableTest extends MyWritableTestBase {

    @Test
    public void test() throws IOException {

        IntWritable intWritable = new IntWritable(163);
        byte[] bytes = serialize(intWritable);
        System.out.println(Integer.toBinaryString(163));
        System.out.println(Integer.toHexString(163));

        assertThat(bytes.length, CoreMatchers.is(4));
        assertEquals(bytes.length, 4);

        assertEquals(StringUtils.byteToHexString(bytes), "000000a3");


        IntWritable newWritable = new IntWritable();
        deserialize(newWritable, bytes);

        assertEquals(newWritable.get(), 163);

    }

}
