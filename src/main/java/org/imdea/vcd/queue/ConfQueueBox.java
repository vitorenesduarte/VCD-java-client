package org.imdea.vcd.queue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.Dots;

/**
 *
 * @author Vitor Enes
 */
public class ConfQueueBox {

    private final Dots dots;
    private final MessageMap messageMap;

    public ConfQueueBox(Dot dot, List<Message> messages) {
        this.dots = new Dots(dot);
        this.messageMap = new MessageMap(dot, messages);
    }

    public ConfQueueBox(ConfQueueBox box) {
        this.dots = new Dots(box.dots);
        this.messageMap = new MessageMap(box.messageMap);
    }

    public Dots getDots() {
        return this.dots;
    }

    public void merge(ConfQueueBox o) {
        this.dots.merge(o.dots);
        this.messageMap.merge(o.messageMap);
    }

    public List<Message> sortMessages() {
        List<Message> result = new ArrayList<>();
        for (PerColor perColor : this.messageMap.map.values()) {
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

        private final HashMap<ByteString, PerColor> map;

        public MessageMap(Dot dot, List<Message> messages) {
            this.map = new HashMap<>();
            this.addAll(dot, messages);
        }

        public MessageMap(MessageMap messageMap) {
            this.map = new HashMap<>();
            for (Map.Entry<ByteString, PerColor> entry : messageMap.map.entrySet()) {
                this.map.put(entry.getKey(), (PerColor) entry.getValue().clone());
            }
        }

        public void merge(MessageMap o) {
            BiFunction<PerColor, PerColor, PerColor> f = (a, b) -> {
                a.merge(b);
                return a;
            };
            for (Map.Entry<ByteString, PerColor> entry : o.map.entrySet()) {
                this.map.merge(entry.getKey(), entry.getValue(), f);
            }
        }

        private void addAll(Dot dot, List<Message> messages) {
            for (Message m : messages) {
                // find message color
                if (m.getHashesCount() != 1) {
                    throw new RuntimeException("Unnexpected number of hashes!");
                }
                ByteString color = m.getHashes(0);

                // check if we already have messages of this color
                PerColor p = this.map.get(color);
                if (p == null) {
                    // if not, create, add, and put it in the map
                    p = new PerColor();
                    p.add(dot, m);
                    this.map.put(color, p);
                } else {
                    // if yes, simply add
                    p.add(dot, m);
                }
            }
        }

        @Override
        public Object clone() {
            MessageMap messageMap = new MessageMap(this);
            return messageMap;
        }
    }

    private class PerColor {

        // within a color, cycles are sorted using the dot
        // - if batching is used, we may have more than one message associated
        // to each dot
        private final TreeMap<Dot, List<Message>> map;

        public PerColor() {
            this.map = new TreeMap<>();
        }

        public PerColor(PerColor perColor) {
            this.map = new TreeMap<>(perColor.map);
        }

        public void add(Dot dot, Message message) {
            // check if we already have messages of this dot
            List<Message> messages = map.get(dot);
            if (messages == null) {
                // if not, create, add, and put it in the map
                messages = new ArrayList<>();
                messages.add(message);
                this.map.put(dot, messages);
            } else {
                // if yes, simply add
                messages.add(message);
            }
        }

        public void merge(PerColor o) {
            this.map.putAll(o.map);
        }

        public List<Message> sortMessages() {
            List<Message> result = new ArrayList<>();
            for (List<Message> v : map.values()) {
                result.addAll(v);
            }
            return result;
        }

        @Override
        public PerColor clone() {
            PerColor perColor = new PerColor(this);
            return perColor;
        }
    }
}
