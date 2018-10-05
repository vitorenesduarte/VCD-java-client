package org.imdea.vcd.queue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.Dots;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.MaxInt;

/**
 *
 * @author Vitor Enes
 */
public class ConfQueueBox {

    private final Dots dots;
    private final Clock<MaxInt> conf;
    private final MessageMap messageMap;

    public ConfQueueBox(Integer nodeNumber) {
        this.dots = new Dots();
        MaxInt bottom = new MaxInt();
        this.conf = new Clock<>(nodeNumber, bottom);
        this.messageMap = new MessageMap();
    }
    
    public ConfQueueBox(Dot dot, Message message, Clock<MaxInt> dep) {
        this.dots = new Dots(dot);
        this.conf = new Clock(dep);
        this.messageMap = new MessageMap(dot, message);
    }

    public ConfQueueBox(ConfQueueBox box) {
        this.dots = new Dots(box.dots);
        this.conf = new Clock(box.conf);
        this.messageMap = new MessageMap(box.messageMap);
    }

    public Dots getDots() {
        return this.dots;
    }

    public boolean before(ConfQueueBox o) {
        return o.conf.intersects(this.dots);
    }

    public void merge(ConfQueueBox o) {
        this.dots.merge(o.dots);
        this.conf.merge(o.conf);
        this.messageMap.merge(o.messageMap);
    }

    public boolean canDeliver(Clock<ExceptionSet> delivered) {
        // delivered will be mutated, adding the dots from this box
        // - in case it can deliver, delivered will be the next queue clock
        delivered.addDots(this.dots);
        return delivered.equals(this.conf);
    }

    public List<Message> sortMessages() {
        List<Message> result = new ArrayList<>();
        for (PerColor perColor : this.messageMap.messages.values()) {
            // sort for each color
            result.addAll(perColor.sortMessages());
        }
        return result;
    }

    public int size() {
        return this.dots.size();
    }

    @Override
    public String toString() {
        return dots.toString();
    }

    @Override
    public Object clone() {
        ConfQueueBox committedQueueBox = new ConfQueueBox(this);
        return committedQueueBox;
    }

    private class MessageMap {

        private final HashMap<ByteString, PerColor> messages;

        public MessageMap() {
            this.messages = new HashMap<>();
        }

        public MessageMap(Dot dot, Message message) {
            this.messages = new HashMap<>();
            PerColor p = new PerColor(dot, message);
            if (message.getHashesCount() > 1) {
                throw new RuntimeException("Number of hashes is bigger than 1");
            }
            this.messages.put(message.getHashes(0), p);
        }

        public MessageMap(MessageMap messageMap) {
            this.messages = new HashMap<>();
            for (Map.Entry<ByteString, PerColor> entry : messageMap.messages.entrySet()) {
                this.messages.put(entry.getKey(), (PerColor) entry.getValue().clone());
            }
        }

        public void merge(MessageMap o) {
            BiFunction<PerColor, PerColor, PerColor> f = (a, b) -> {
                a.merge(b);
                return a;
            };
            for (Map.Entry<ByteString, PerColor> entry : o.messages.entrySet()) {
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
