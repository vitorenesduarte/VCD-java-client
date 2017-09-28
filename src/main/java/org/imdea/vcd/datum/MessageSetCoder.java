package org.imdea.vcd.datum;

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
 *
 * As explained here:
 * https://cwiki.apache.org/confluence/display/AVRO/FAQ#FAQ-HowcanIserializedirectlyto/fromabytearray?
 *
 */
public class MessageSetCoder {

    private static final Schema SCHEMA = MessageSet.getClassSchema();
    private final SpecificDatumReader<MessageSet> reader;
    private final SpecificDatumWriter<MessageSet> writer;

    public MessageSetCoder() {
        this.reader = new SpecificDatumReader<>(SCHEMA);
        this.writer = new SpecificDatumWriter<>(SCHEMA);
    }

    public byte[] encode(MessageSet messageSet) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        this.writer.write(messageSet, encoder);

        encoder.flush();
        out.close();

        byte[] data = out.toByteArray();
        return data;
    }

    public MessageSet decode(byte[] data) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        MessageSet messageSet = this.reader.read(null, decoder);
        return messageSet;
    }
}
