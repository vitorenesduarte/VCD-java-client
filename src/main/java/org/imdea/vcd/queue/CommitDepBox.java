package org.imdea.vcd.queue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.MaxInt;
import org.imdea.vcd.pb.Proto.Commit;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Dot;
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
        this.dots = new Dots(Dot.dot(commit.getDot()));
        this.dep = Clock.eclock(commit.getDepMap());
        this.messageMap = new MessageMap(commit);
    }

    public CommitDepBox(Dot dot, Clock<ExceptionSet> dep, Message message, Clock<MaxInt> conf) {
        this.dots = new Dots(dot);
        this.dep = dep;
        this.messageMap = new MessageMap(dot, message, conf);
    }

    public CommitDepBox(CommitDepBox commitDepBox) {
        this.dots = new Dots(commitDepBox.dots);
        this.dep = new Clock<>(commitDepBox.dep);
        this.messageMap = new MessageMap(commitDepBox.messageMap);
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

    public Dots getDots() {
        return this.dots;
    }

    public List<Message> sortMessages() {
        List<Message> result = new ArrayList<>();
        for (ArrayList<PerMessage> messages : this.messageMap.messages.values()) {
            // sort for each color
            result.addAll(sortPerColor(messages));
        }
        return result;
    }

    private List<Message> sortPerColor(List<PerMessage> messages) {
        // create queue to sort messages
        Integer nodeNumber = messages.get(0).getConf().size();
        DependencyQueue<DeliveredDepBox> queue = new DependencyQueue<>(nodeNumber);

        // add all to the queue
        for (PerMessage message : messages) {
            DeliveredDepBox box = new DeliveredDepBox(message);
            queue.add(box);
        }

        // take all messages in the queue
        List<Message> result = new ArrayList<>();
        for (DeliveredDepBox box : queue.toList()) {
            result.addAll(box.sortMessages());
        }
        return result;
    }

    @Override
    public String toString() {
        return dots + " " + dep;
    }

    @Override
    public Object clone() {
        CommitDepBox commitDepBox = new CommitDepBox(this);
        return commitDepBox;
    }

    private class MessageMap {

        private final TreeMap<String, ArrayList<PerMessage>> messages;

        public MessageMap(Commit commit) {
            this(Dot.dot(commit.getDot()), commit.getMessage(), Clock.vclock(commit.getConfMap()));
        }

        public MessageMap(Dot dot, Message message, Clock<MaxInt> conf) {
            this.messages = new TreeMap<>();
            String color = message.getHash().toString();
            PerMessage p = new PerMessage(dot, message, conf);
            this.messages.put(color, new ArrayList<>(Arrays.asList(p)));
        }

        public MessageMap(MessageMap messageMap) {
            this.messages = new TreeMap<>();
            for (Map.Entry<String, ArrayList<PerMessage>> entry : messageMap.messages.entrySet()) {
                ArrayList<PerMessage> perMessageList = new ArrayList<>();
                for (PerMessage perMessage : entry.getValue()) {
                    perMessageList.add((PerMessage) perMessage.clone());
                }
                this.messages.put(entry.getKey(), perMessageList);
            }
        }

        public void merge(MessageMap o) {
            BiFunction<ArrayList<PerMessage>, ArrayList<PerMessage>, ArrayList<PerMessage>> f = (a, b) -> {
                a.addAll(b);
                return a;
            };
            for (Map.Entry<String, ArrayList<PerMessage>> entry : o.messages.entrySet()) {
                this.messages.merge(entry.getKey(), entry.getValue(), f);
            }
        }

        @Override
        public Object clone() {
            MessageMap messageMap = new MessageMap(this);
            return messageMap;
        }
    }
}
