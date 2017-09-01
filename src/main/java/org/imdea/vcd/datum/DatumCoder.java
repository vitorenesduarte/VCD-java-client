package org.imdea.vcd.datum;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 *
 * @author Vitor Enes
 *
 * As explained here:
 * https://cwiki.apache.org/confluence/display/AVRO/FAQ#FAQ-HowcanIserializedirectlyto/fromabytearray?
 *
 */
public class DatumCoder {

    public static byte[] encode(DatumType type, Object record) throws IOException {
        //Debug.start("Encode");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        type.getWriter().write(record, encoder);

        encoder.flush();
        out.close();

        byte[] data = out.toByteArray();
        //Debug.end("Encode");
        return data;
    }

    public static Object decode(DatumType type, byte[] data) throws IOException {
        //Debug.start("Decode");
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        Object record = type.getReader().read(null, decoder);
        //Debug.end("Decode");
        return record;
    }
}
