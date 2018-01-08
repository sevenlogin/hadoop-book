// == WritableTestBase-Deserialize

import org.apache.hadoop.io.Writable;

import java.io.*;

public class MyWritableTestBase {

    public byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    public byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
        InputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }
}
