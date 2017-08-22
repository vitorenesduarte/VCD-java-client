package org.imdea.vcd;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 *
 * @author Vitor Enes
 * @param <T>
 *
 * As explained here:
 * https://cwiki.apache.org/confluence/display/AVRO/FAQ#FAQ-HowcanIserializedirectlyto/fromabytearray?
 *
 */
public class EncoderDecoder<T extends org.apache.avro.specific.SpecificRecordBase> {

    private final SpecificDatumWriter<T> writer;
    private final SpecificDatumReader<T> reader;

    public EncoderDecoder(Schema schema) {
        this.writer = new SpecificDatumWriter<>(schema);
        this.reader = new SpecificDatumReader<>(schema);
    }

    public byte[] encode(T t) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        writer.write(t, encoder);
        
        encoder.flush();
        out.close();

        byte[] data = out.toByteArray();
        return data;
    }

    public T decode(byte[] data) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        
        T t = reader.read(null, decoder);
        
        return t;
    }
}
