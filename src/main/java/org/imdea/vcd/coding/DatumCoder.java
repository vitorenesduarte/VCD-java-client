package org.imdea.vcd.coding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.imdea.vcd.MessageSet;

/**
 *
 * @author Vitor Enes
 *
 * As explained here:
 * https://cwiki.apache.org/confluence/display/AVRO/FAQ#FAQ-HowcanIserializedirectlyto/fromabytearray?
 *
 */
public class DatumCoder implements Coder {

    public DatumCoder() {
    }

    @Override
    public byte[] encode(MessageSet messageSet) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        SpecificDatumWriter<MessageSet> writer = new SpecificDatumWriter<>(MessageSet.getClassSchema());
        writer.write(messageSet, encoder);

        encoder.flush();
        out.close();

        byte[] data = out.toByteArray();
        return data;
    }

    @Override
    public MessageSet decode(byte[] data) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

        SpecificDatumReader<MessageSet> reader = new SpecificDatumReader<>(MessageSet.getClassSchema());
        MessageSet messageSet = reader.read(null, decoder);

        return messageSet;
    }
}
