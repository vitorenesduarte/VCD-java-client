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
import org.imdea.vcd.datum.MessageSetCoder;
import org.imdea.vcd.datum.MessageSet;
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
        MessageSetCoder coder = new MessageSetCoder();
        for (int i = 0; i < REPETITIONS; i++) {
            MessageSet record = RandomMessageSet.generate();

            this.rw.write(coder.encode(record));
            MessageSet result = coder.decode(this.rw.read());

            Assert.assertEquals(record, result);
        }
    }
}
