package org.imdea.vcd.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.imdea.vcd.VCDLogger;
import org.imdea.vcd.pb.Proto;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 *
 * >>> Types:
 *
 * Msg: <{Color}, Data>
 *
 * MsgSet: <STATUS, [Msg]>
 *
 * pack([Msg]): MsgSet
 *
 * unpack(MsgSet): MsgSet
 *
 *
 * >>> Example:
 *
 * X = [<{red}, p1>, <{blue}, p2>]
 *
 * pack(X) = <START, [<{red, blue}, X>]>
 *
 * Y = <DURABLE, [<{red, blue}, X>]>
 *
 * unpack(Y) = <DURABLE, X>
 *
 */
public class Batch {

    private static final Logger LOGGER = VCDLogger.init(Batch.class);

    public static MessageSet pack(Message m) {
        return pack(Collections.singletonList(m));
    }

    public static MessageSet pack(List<Message> ops) {
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
        Proto.Message theMessage = Proto.Message.newBuilder()
                .addAllHashes(hashes)
                .setData(batch.toByteString())
                .build();

        // create a message set to be pushed to the server
        MessageSet messageSet = MessageSet.newBuilder()
                .addMessages(theMessage)
                .setStatus(MessageSet.Status.START)
                .build();
        return messageSet;
    }

    public static List<Message> unpack(Message m) throws InvalidProtocolBufferException {
        // parse protobuf inside message
        MessageSet batch = MessageSet.parseFrom(m.getData());
        return batch.getMessagesList();
    }

    public static MessageSet unpack(MessageSet ms) throws InvalidProtocolBufferException {
        if (ms.getMessagesCount() != 1) {
            LOGGER.log(Level.WARNING, "[unpacking] Expecting a message set with a single message!!");
        }
        // find message
        Proto.Message m = ms.getMessages(0);

        // unpack it
        List<Message> messages = unpack(m);

        // build a message with the messages in the batch
        // and keep the same status
        MessageSet messageSet = MessageSet.newBuilder()
                .addAllMessages(messages)
                .setStatus(ms.getStatus())
                .build();
        return messageSet;
    }
}
