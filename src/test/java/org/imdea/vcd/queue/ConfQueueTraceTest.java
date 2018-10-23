package org.imdea.vcd.queue;

import com.google.protobuf.ByteString;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.imdea.vcd.Generator;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.MaxInt;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.imdea.vcd.Trace;
import org.imdea.vcd.queue.clock.Dots;
import org.imdea.vcd.queue.clock.ExceptionSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;

/**
 *
 * @author Vitor Enes
 */
@Ignore
public class ConfQueueTraceTest {

    public static final String TRACE_FILE = "/Users/user/IMDEA/bin/logs/5/trace-northamerica-northeast1-b";

    @Test
    public void testTrace() throws IOException {
        Clock<ExceptionSet> committed = null;
        List<QueueAddArgs> argsList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(TRACE_FILE))) {
            for (String line; (line = br.readLine()) != null;) {
                Trace trace = Trace.decode(line);
                switch (trace.getType()) {
                    case COMMITTED:
                        committed = trace.getCommitted();
                        System.out.println(committed);
                        break;
                    case COMMIT:
                        argsList.add(args(trace.getDot(), trace.getConf()));
                        System.out.println(trace.getDot() + " " + trace.getConf());
                        break;
                }
            }
        }
        checkTermination(committed, argsList);
    }

    private Map<Dots, List<Message>> checkTermination(Object queueArg, List<QueueAddArgs> argsList) {
        ConfQueue queue;
        if (queueArg instanceof Integer) {
            queue = new ConfQueue((Integer) queueArg);
        } else {
            queue = new ConfQueue((Clock<ExceptionSet>) queueArg);
        }

        return checkTermination(queue, argsList);
    }

    private Map<Dots, List<Message>> checkTermination(ConfQueue queue, List<QueueAddArgs> argsList) {
        // results
        List<ConfQueueBox> results = new ArrayList<>();

        // add all to queue
        for (QueueAddArgs args : argsList) {
            QueueAddArgs copy = (QueueAddArgs) args.clone();
            queue.add(copy.getDot(), copy.getMessage(), copy.getConf());
            List<ConfQueueBox> result = queue.getToDeliver();
            results.addAll(result);
        }

        // check queue is empty and all dots were delivered
        boolean emptyQueue = queue.isEmpty() && queue.elements() == 0;
        boolean allDots = checkAllDotsDelivered(argsList, results);
        boolean termination = emptyQueue && allDots;

        if (!termination) {
            System.out.println(argsList);
        }
        assertTrue(termination);

        // return messages sorted
        Map<Dots, List<Message>> sorted = new HashMap<>();
        for (ConfQueueBox box : results) {
            sorted.put(box.getDots(), box.sortMessages());
        }
        return sorted;
    }

    private boolean checkAllDotsDelivered(List<QueueAddArgs> argsList, List<ConfQueueBox> results) {
        List<Dot> dots = new ArrayList<>();
        for (QueueAddArgs args : argsList) {
            dots.add(args.getDot());
        }

        List<Dot> resultsDots = boxListToDots(results);

        // all dots (and no more) were delivered
        return dots.size() == resultsDots.size() && resultsDots.containsAll(dots);
    }

    private List<Dot> boxListToDots(List<ConfQueueBox> list) {
        List<Dot> dots = new ArrayList<>();
        for (ConfQueueBox e : list) {
            for (Dot dot : e.getDots()) {
                dots.add(dot);
            }
        }
        return dots;
    }

    private QueueAddArgs args(Dot dot, Clock<MaxInt> conf) {
        conf.removeDot(dot);
        QueueAddArgs args = new QueueAddArgs(dot, conf);
        return args;
    }

    private void checkTotalOrderPerColor(Map<Dots, List<Message>> ma, Map<Dots, List<Message>> mb) {
        for (Map.Entry<Dots, List<Message>> entry : ma.entrySet()) {
            List<Message> a = entry.getValue();
            List<Message> b = mb.get(entry.getKey());
            checkTotalOrderPerColor(a, b);
        }
    }

    private void checkTotalOrderPerColor(List<Message> a, List<Message> b) {
        HashSet<ByteString> colors = new HashSet<>();

        for (Message m : a) {
            colors.add(m.getHashes(0));
        }

        for (ByteString color : colors) {
            List<Message> perColorA = messagesPerColor(color, a);
            List<Message> perColorB = messagesPerColor(color, b);
            assertEquals(perColorA, perColorB);
        }
    }

    private List<Message> messagesPerColor(ByteString color, List<Message> l) {
        List<Message> perColor = new ArrayList<>();
        for (Message m : l) {
            if (m.getHashes(0).equals(color)) {
                perColor.add(m);
            }
        }
        return perColor;
    }

    public Clock<MaxInt> vclock(Long... seqs) {
        HashMap<Integer, MaxInt> map = new HashMap<>();
        for (Integer i = 0; i < seqs.length; i++) {
            map.put(i, new MaxInt(seqs[i]));
        }
        return new Clock<>(map);
    }

    public Clock<ExceptionSet> eclock(Long... seqs) {
        HashMap<Integer, ExceptionSet> map = new HashMap<>();
        for (Integer i = 0; i < seqs.length; i++) {
            map.put(i, new ExceptionSet(seqs[i]));
        }
        return new Clock<>(map);
    }

    private class QueueAddArgs {

        private final Dot dot;
        // build black message
        private final Message message;
        private final Clock<MaxInt> conf;

        public QueueAddArgs(Dot dot, Clock<MaxInt> conf) {
            this.dot = dot;
            this.message = Generator.message("black");
            this.conf = conf;
        }

        public QueueAddArgs(QueueAddArgs o) {
            this.dot = new Dot(o.dot);
            this.message = Message.newBuilder()
                    .addAllHashes(o.message.getHashesList())
                    .setData(o.message.getData())
                    .build();
            this.conf = new Clock(o.conf);
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

        @Override
        public String toString() {
            return dot + " " + conf;
        }

        @Override
        public QueueAddArgs clone() {
            QueueAddArgs args = new QueueAddArgs(this);
            return args;
        }
    }
}
