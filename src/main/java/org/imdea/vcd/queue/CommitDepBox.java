package org.imdea.vcd.queue;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
    public Proto.MessageSet toMessageSet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private class MessageMap {

        private final HashMap<ByteString, List<PerMessage>> messages;

        public MessageMap(Commit commit) {
            this.messages = new HashMap<>();
            ByteString color = commit.getMessage().getHash();
            PerMessage p = new PerMessage(commit);
            this.messages.put(color, Arrays.asList(p));
        }

        public void merge(MessageMap o) {
            BiFunction<List<PerMessage>, List<PerMessage>, List<PerMessage>> f = (a, b) -> {
                a.addAll(b);
                return a;
            };
            for (Map.Entry<ByteString, List<PerMessage>> entry : o.messages.entrySet()) {
                this.messages.merge(entry.getKey(), entry.getValue(), f);
            }
        }
    }

    private class PerMessage {

        private final Dot dot;
        private final Message message;
        private final Clock<MaxInt> conf;

        public PerMessage(Commit commit) {
            this.dot = commit.getDot();
            this.message = commit.getMessage();
            this.conf = Clock.vclock(commit.getConfMap());
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
