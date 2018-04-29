package org.imdea.vcd;

import org.imdea.vcd.queue.DependencyQueue;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.imdea.vcd.pb.Proto.MessageSet;
import org.imdea.vcd.pb.Proto.Reply;
import org.imdea.vcd.queue.CommitDepBox;

/**
 *
 * @author Vitor Enes
 */
public class DataRW {

    private final DataInputStream in;
    private final DataOutputStream out;
    private final DependencyQueue<CommitDepBox> queue;

    public DataRW(DataInputStream in, DataOutputStream out) {
        this.in = in;
        this.out = out;
        this.queue = new DependencyQueue<>();
    }

    public void write(MessageSet messageSet) throws IOException {
        byte[] data = messageSet.toByteArray();
        out.writeInt(data.length);
        out.write(data, 0, data.length);
        out.flush();
    }

    public MessageSet read() throws IOException {
        int length = in.readInt();
        byte data[] = new byte[length];
        in.readFully(data, 0, length);

        // check if reply is a message set
        // if yes, return it,
        // otherwise feed the dependency queue
        // with the commit notification
        Reply reply = Reply.parseFrom(data);
        switch (reply.getReplyCase()) {
            case SET:
                return reply.getSet();
            case COMMIT:
                CommitDepBox box = new CommitDepBox(reply.getCommit());
                queue.add(box);
                MessageSet messageSet = box.toMessageSet();
                if (messageSet != null) {
                    return messageSet;
                } else {
                    return this.read();
                }
            default:
                throw new RuntimeException("Reply type not supported:" + reply.getReplyCase());
        }
    }

    public void close() throws IOException {
        this.in.close();
        this.out.close();
    }
}
