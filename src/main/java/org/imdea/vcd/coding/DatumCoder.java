package org.imdea.vcd.coding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.imdea.vcd.Debug;
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

    private static final SpecificDatumWriter<MessageSet> WRITER = new SpecificDatumWriter<>(MessageSet.getClassSchema());
    private static final SpecificDatumReader<MessageSet> READER = new SpecificDatumReader<>(MessageSet.getClassSchema());

    public DatumCoder() {
    }

    @Override
    public byte[] encode(MessageSet messageSet) throws IOException {
        //Debug.start("Encode");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        WRITER.write(messageSet, encoder);

        encoder.flush();
        out.close();

        byte[] data = out.toByteArray();
        //Debug.end("Encode");
        return data;
    }

    @Override
    public MessageSet decode(byte[] data) throws IOException {
        //Debug.start("Decode");
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        MessageSet messageSet = READER.read(null, decoder);
        //Debug.end("Decode");
        return messageSet;
    }
}
