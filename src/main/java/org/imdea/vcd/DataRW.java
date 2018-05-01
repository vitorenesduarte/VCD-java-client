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
        MessageSet result = null;
        while (result == null) {
            result = doRead();
        }
        return result;
    }

    private MessageSet doRead() throws IOException {
        int length = in.readInt();
        byte data[] = new byte[length];
        in.readFully(data, 0, length);

        long start;

        // check if reply is a message set
        // if yes, return it,
        // otherwise feed the dependency queue
        // with the commit notification
        Reply reply = Reply.parseFrom(data);
        switch (reply.getReplyCase()) {
            case INIT:
                this.queue = new DependencyQueue(Clock.eclock(reply.getInit().getCommittedMap()));
                return null;
            case SET:
                return reply.getSet();
            case COMMIT:
                start = System.nanoTime();
                CommitDepBox box = new CommitDepBox(reply.getCommit());
                System.out.println((System.nanoTime() - start) + " create box");

                start = System.nanoTime();
                queue.add(box);
                System.out.println((System.nanoTime() - start) + " add box");

                start = System.nanoTime();
                List<CommitDepBox> toDeliver = queue.tryDeliver();
                System.out.println((System.nanoTime() - start) + " try deliver");

                if (toDeliver.isEmpty()) {
                    return null;
                } else {
                    start = System.nanoTime();
                    MessageSet.Builder builder = MessageSet.newBuilder();
                    for (CommitDepBox boxToDeliver : toDeliver) {
                        for (Message message : boxToDeliver.sortMessages()) {
                            builder.addMessages(message);
                        }
                    }
                    builder.setStatus(MessageSet.Status.DELIVERED);
                    MessageSet messageSet = builder.build();
                    System.out.println((System.nanoTime() - start) + " sorting " + messageSet.getMessagesCount() + " messages");
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
