package org.imdea.vcd.util;

import com.google.protobuf.InvalidProtocolBufferException;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 *
 */
public class Batch {

    public static Message pack(Message message) {
        return message;
    }

    public static Message unpack(Message message) throws InvalidProtocolBufferException {
        return message;
    }

    public static MessageSet unpack(Message message, MessageSet.Status status) throws InvalidProtocolBufferException {
        // build a message with the messages in the batch
        MessageSet messageSet = MessageSet.newBuilder()
                .addMessages(message)
                .setStatus(status)
                .build();
        return messageSet;
    }
}
