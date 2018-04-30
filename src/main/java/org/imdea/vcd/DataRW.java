package org.imdea.vcd;

import org.imdea.vcd.queue.DependencyQueue;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;
import org.imdea.vcd.pb.Proto.Reply;
import org.imdea.vcd.queue.CommitDepBox;
import org.imdea.vcd.queue.clock.Clock;

/**
 *
 * @author Vitor Enes
 */
public class DataRW {

    private final DataInputStream in;
    private final DataOutputStream out;
    private DependencyQueue<CommitDepBox> queue;

    public DataRW(DataInputStream in, DataOutputStream out, Integer nodeNumber) {
        this.in = in;
        this.out = out;
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
            case INIT:
                this.queue = new DependencyQueue(Clock.eclock(reply.getInit().getCommittedMap()));
                return this.read();
            case SET:
                return reply.getSet();
            case COMMIT:
                CommitDepBox box = new CommitDepBox(reply.getCommit());
                List<CommitDepBox> toDeliver = queue.add(box);

                if (toDeliver.isEmpty()) {
                    return this.read();
                } else {
                    MessageSet.Builder builder = MessageSet.newBuilder();
                    for (CommitDepBox boxToDeliver : toDeliver) {
                        for (Message message : boxToDeliver.allMessages()) {
                            builder.addMessages(message);
                        }
                    }
                    builder.setStatus(MessageSet.Status.DELIVERED);
                    MessageSet messageSet = builder.build();
                    return messageSet;
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
