package org.imdea.vcd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.imdea.vcd.datum.DatumCoder;
import static org.imdea.vcd.datum.DatumType.MESSAGE_SET;
import static org.imdea.vcd.datum.DatumType.STATUS;
import org.imdea.vcd.datum.MessageSet;
import org.imdea.vcd.datum.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Vitor Enes
 */
public class CoderTest {

    private final Integer REPETITIONS = 1000; // there's probably a better way
    private final String FILE = "/tmp/avro";
    private DataRW rw;

    @Before
    public void beforeTest() throws IOException {
        Path path = Paths.get(FILE);
        try {
            Files.delete(path);
        } catch (java.nio.file.NoSuchFileException e) {
            // swallow
        }
        Files.createFile(path);

        DataInputStream in = new DataInputStream(new FileInputStream(FILE));
        DataOutputStream out = new DataOutputStream(new FileOutputStream(FILE));
        this.rw = new DataRW(in, out);
    }

    @Test
    public void testMessageSetEncodeDecode() throws FileNotFoundException, IOException {
        for (int i = 0; i < REPETITIONS; i++) {
            MessageSet record = RandomMessageSet.generate();

            this.rw.write(DatumCoder.encode(MESSAGE_SET, record));
            MessageSet result = (MessageSet) DatumCoder.decode(MESSAGE_SET, this.rw.read());

            Assert.assertEquals(record, result);
        }
    }

    @Test
    public void testStatusEncodeDecode() throws IOException, IOException {
        Status record = Status.COMMITTED;

        this.rw.write(DatumCoder.encode(STATUS, record));
        Status result = (Status) DatumCoder.decode(STATUS, this.rw.read());

        Assert.assertEquals(record, result);
    }
}
