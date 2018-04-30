package org.imdea.vcd.queue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.MaxInt;
import org.imdea.vcd.pb.Proto;
import org.imdea.vcd.pb.Proto.Commit;
import org.imdea.vcd.pb.Proto.Dot;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Dots;

/**
 *
 * @author Vitor Enes
 */
public class CommitDepBox implements DepBox<CommitDepBox> {

    private final Dots dots;
    private final Clock<ExceptionSet> dep;
    private final MessageMap messageMap;

    public CommitDepBox(Commit commit) {
        this.dots = new Dots(commit.getDot());
        this.dep = Clock.eclock(commit.getDepMap());
        this.messageMap = new MessageMap(commit);
    }

    public CommitDepBox(Dot dot, Clock<ExceptionSet> dep, Message message, Clock<MaxInt> conf) {
        this.dots = new Dots(dot);
        this.dep = dep;
        this.messageMap = new MessageMap(dot, message, conf);
    }

    @Override
    public boolean before(CommitDepBox o) {
        return o.dep.intersects(this.dots);
    }

    @Override
    public void merge(CommitDepBox o) {
        this.dots.merge(o.dots);
        this.dep.merge(o.dep);
        this.messageMap.merge(o.messageMap);
    }

    @Override
    public boolean canDeliver(Clock<ExceptionSet> delivered) {
        // delivered will be mutated, adding the dots from this box
        // - in case it can deliver, delivered will be the next queue clock
        delivered.addDots(this.dots);
        return delivered.equals(this.dep);
    }

    @Override
    public Proto.MessageSet toMessageSet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private class MessageMap {

        private final HashMap<ByteString, ArrayList<PerMessage>> messages;

        public MessageMap(Commit commit) {
            this(commit.getDot(), commit.getMessage(), Clock.vclock(commit.getConfMap()));
        }

        public MessageMap(Dot dot, Message message, Clock<MaxInt> conf) {
            this.messages = new HashMap<>();
            ByteString color = message.getHash();
            PerMessage p = new PerMessage(dot, message, conf);
            this.messages.put(color, new ArrayList<>(Arrays.asList(p)));
        }

        public void merge(MessageMap o) {
            BiFunction<ArrayList<PerMessage>, ArrayList<PerMessage>, ArrayList<PerMessage>> f = (a, b) -> {
                a.addAll(b);
                return a;
            };
            for (Map.Entry<ByteString, ArrayList<PerMessage>> entry : o.messages.entrySet()) {
                this.messages.merge(entry.getKey(), entry.getValue(), f);
            }
        }
    }

    private class PerMessage {

        private final Dot dot;
        private final Message message;
        private final Clock<MaxInt> conf;

        public PerMessage(Dot dot, Message message, Clock<MaxInt> conf) {
            this.dot = dot;
            this.message = message;
            this.conf = conf;
        }

        public Dot getDot() {
            return dot;
        }

        public Message getMessage() {
            return message;
        }

        public Clock<MaxInt> getConf() {
            return conf;
        }
    }
}
