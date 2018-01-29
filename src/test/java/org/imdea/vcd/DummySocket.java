package org.imdea.vcd;

import org.imdea.vcd.pb.Proto;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class DummySocket extends Socket{

    BlockingQueue<Proto.MessageSet> queue;

    private DummySocket(DataRW rw) {
        super(rw);
        queue = new LinkedBlockingDeque<>();
    }

    public static Socket create(Config config) throws IOException {
        return new DummySocket(null);
    }

    public void send(Proto.MessageSet messageSet) throws IOException {
        Proto.MessageSet.Builder builder = Proto.MessageSet.newBuilder();

        for (Proto.Message message : messageSet.getMessagesList()) {
            builder.addMessages(message);
        }

        //Proto.MessageSet.Builder cloned = messageSet.toBuilder().clone();

        try {
            builder.setStatus(Proto.MessageSet.Status.COMMITTED);
            queue.put(builder.build());

            builder.setStatus(Proto.MessageSet.Status.DELIVERED);
            queue.put(builder.build());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Proto.MessageSet receive() throws IOException {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

}
