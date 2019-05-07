package org.imdea.vcd.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashSet;
import java.util.List;
import org.imdea.vcd.pb.Proto;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 *
 */
public class Batch {

    public static Message pack(List<Message> ops) {
        // create a message set with all messages in the batch
        MessageSet.Builder builder = MessageSet.newBuilder();
        HashSet<ByteString> hashes = new HashSet<>();
        for (Message m : ops) {
            // add message to the message set
            builder.addMessages(m);
            // collect all its hashes
            hashes.addAll(m.getHashesList());
        }
        MessageSet batch = builder.build();

        // create a message that has as data
        // a protobuf with the previous message set
        Proto.Message message = Message.newBuilder()
                .addAllHashes(hashes)
                .setData(batch.toByteString())
                .build();

        return message;
    }

    public static List<Message> unpack(Message m) throws InvalidProtocolBufferException {
        List<Message> messages = MessageSet.parseFrom(m.getData()).getMessagesList();
        return messages;
    }

    public static MessageSet unpack(Message m, MessageSet.Status status) throws InvalidProtocolBufferException {
        List<Message> messages = unpack(m);

        // build a message with the messages in the batch
        MessageSet messageSet = MessageSet.newBuilder()
                .addAllMessages(messages)
                .setStatus(status)
                .build();
        return messageSet;
    }
}
