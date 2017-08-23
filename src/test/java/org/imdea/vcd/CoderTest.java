package org.imdea.vcd;

import org.imdea.vcd.coding.Coder;
import org.imdea.vcd.coding.DatumCoder;
import org.imdea.vcd.coding.SimpleCoder;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Vitor Enes
 */
public class CoderTest {

    private final Integer REPETITIONS = 10000; // there's probably a better way
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
    public void testSimpleEncodeDecode() throws FileNotFoundException, IOException {
        SimpleCoder coder = new SimpleCoder();
        testEncodeDecode(coder);
    }

    @Test
    public void testDatumEncodeDecode() throws FileNotFoundException, IOException {
        DatumCoder coder = new DatumCoder();
        for (int i = 0; i < REPETITIONS; i++) {
            testEncodeDecode(coder);
        }
    }

    @Test
    public void sameEncoding() throws IOException {
        for (int i = 0; i < REPETITIONS; i++) {
            MessageSet ms = RandomMessageSet.generate();

            byte[] expected = new DatumCoder().encode(ms);
            byte[] result = new SimpleCoder().encode(ms);

            Assert.assertArrayEquals(expected, result);
        }
    }

    private void testEncodeDecode(Coder coder) throws IOException, IOException {
        MessageSet ms = RandomMessageSet.generate();

        this.rw.write(coder.encode(ms));
        MessageSet result = coder.decode(this.rw.read());

        Assert.assertEquals(ms, result);
    }
}
