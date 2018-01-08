import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class MyAvroTest {

    @Test
    public void testInt() throws IOException {
        Schema schema = new Schema.Parser().parse("\"int\"");
        int data = 1111;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<Integer> writer = new GenericDatumWriter<Integer>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
        writer.write(data, encoder); // boxed
        encoder.flush();
        out.close();

        DatumReader<Integer> reader = new GenericDatumReader<Integer>(schema); // have to tell it the schema - it's not in the data stream!
        Decoder decoder = DecoderFactory.get()
                .binaryDecoder(out.toByteArray(), null /* reuse */);
        Integer result = reader.read(null /* reuse */, decoder);
        assertThat(result, is(163));

        try {
            reader.read(null, decoder);
            fail("Expected EOFException");
        } catch (EOFException e) {
            // expected
        }
    }

    @Test
    public void testPairGeneric() throws Exception {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));

        //data
        GenericRecord record = new GenericData.Record(schema);
        record.put("left", "L");
        record.put("right", "R");

        //serialize
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        writer.write(record, encoder);
        encoder.flush();
        out.close();

        //deserialize
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord result = reader.read(null, decoder);
        assertEquals(result.get("left").toString(), "L");
        assertEquals(result.get("right").toString(), "R");
    }

    @Test
    public void testPairSpecial() throws Exception {

        //data
        StringPair data = new StringPair();
        data.setLeft("L");
        data.setRight("R");

        //serialize
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        SpecificDatumWriter<StringPair> writer = new SpecificDatumWriter<StringPair>(data.getSchema());
        //SpecificDatumWriter<StringPair> writer = new SpecificDatumWriter<StringPair>(StringPair.class);
        writer.write(data, encoder);
        encoder.flush();
        out.close();

        //deserialize
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        SpecificDatumReader<StringPair> reader = new SpecificDatumReader<StringPair>(data.getSchema());
//        SpecificDatumReader<StringPair> reader = new SpecificDatumReader<StringPair>(StringPair.class);
        StringPair result = reader.read(null, decoder);
        assertEquals(result.getLeft(), "L");
        assertEquals(result.getRight(), "R");
    }

    @Test
    public void testDataFile() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        File file = new File("data.avro");

        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(datum);
        dataFileWriter.close();

        //deserialize from disk
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);

        assertEquals(schema, dataFileReader.getSchema());

        assertEquals(dataFileReader.hasNext(), true);
        GenericRecord record = dataFileReader.next();
        assertEquals(record.get("left").toString(), "L");
        assertEquals(record.get("right").toString(), "R");
        assertEquals(dataFileReader.hasNext(), false);

    }
}
