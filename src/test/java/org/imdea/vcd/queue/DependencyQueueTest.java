package org.imdea.vcd.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.imdea.vcd.Generator;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.MaxInt;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author user
 */
public class DependencyQueueTest {

    private static final int ITERATIONS = 10;

    @Test
    public void testAdd() {
        Integer nodeNumber = 2;

        // {{0, 2}, [{0, 2}, {1, 2}]}
        Dot dotA = new Dot(0, 2L);
        HashMap<Integer, ExceptionSet> mapA = new HashMap<>();
        mapA.put(0, new ExceptionSet(2L, 1L));
        mapA.put(1, new ExceptionSet(2L, 1L));

        // {{0, 1}, [{0, 1}, {0, 2}, {0, 3}, {1, 2}]}
        Dot dotB = new Dot(0, 1L);
        HashMap<Integer, ExceptionSet> mapB = new HashMap<>();
        mapB.put(0, new ExceptionSet(3L));
        mapB.put(1, new ExceptionSet(2L, 1L));

        // {{0, 5}, [{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {0, 6}, {1, 1}, {1, 2}]}
        Dot dotC = new Dot(0, 5L);
        HashMap<Integer, ExceptionSet> mapC = new HashMap<>();
        mapC.put(0, new ExceptionSet(6L));
        mapC.put(1, new ExceptionSet(2L));

        // {{0, 6}, [{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {0, 6}, {1, 1}, {1, 2}, {1, 3}]}
        Dot dotD = new Dot(0, 6L);
        HashMap<Integer, ExceptionSet> mapD = new HashMap<>();
        mapD.put(0, new ExceptionSet(6L));
        mapD.put(1, new ExceptionSet(3L));

        // {{0, 3}, [{0, 1}, {0, 2}, {0, 3}, {1, 1}, {1, 2}, {1, 3}]}
        Dot dotE = new Dot(0, 3L);
        HashMap<Integer, ExceptionSet> mapE = new HashMap<>();
        mapE.put(0, new ExceptionSet(3L));
        mapE.put(1, new ExceptionSet(3L));

        // {{1, 2}, [{1, 2}]}
        Dot dotF = new Dot(1, 2L);
        HashMap<Integer, ExceptionSet> mapF = new HashMap<>();
        mapF.put(0, new ExceptionSet(0L));
        mapF.put(1, new ExceptionSet(2L, 1L));

        // {{1, 1}, [{0, 1}, {0, 2}, {0, 3}, {0, 4}, {1, 1}, {1, 2}, {1, 3}]}
        Dot dotG = new Dot(1, 1L);
        HashMap<Integer, ExceptionSet> mapG = new HashMap<>();
        mapG.put(0, new ExceptionSet(4L));
        mapG.put(1, new ExceptionSet(3L));

        // {{0, 4}, [{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 6}, {1, 1}, {1, 2}]}
        Dot dotH = new Dot(0, 4L);
        HashMap<Integer, ExceptionSet> mapH = new HashMap<>();
        mapH.put(0, new ExceptionSet(6L, 5L));
        mapH.put(1, new ExceptionSet(2L));

        // {{1, 3}, [{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {0, 6}, {1, 1}, {1, 2}, {1, 3}]}
        Dot dotI = new Dot(1, 3L);
        HashMap<Integer, ExceptionSet> mapI = new HashMap<>();
        mapI.put(0, new ExceptionSet(6L));
        mapI.put(1, new ExceptionSet(3L));

        List<CommitDepBox> boxes = new ArrayList<>();
        boxes.add(box(dotA, mapA));
        boxes.add(box(dotB, mapB));
        boxes.add(box(dotC, mapC));
        boxes.add(box(dotD, mapD));
        boxes.add(box(dotE, mapE));
        boxes.add(box(dotF, mapF));
        boxes.add(box(dotG, mapG));
        boxes.add(box(dotH, mapH));
        boxes.add(box(dotI, mapI));

        checkTerminationRandomShuffles(nodeNumber, boxes);
    }

    @Test
    public void missingTests() {
//    L2 = [{{1, 4}, [{0, 2}, {0, 3}, {1, 1}, {1, 2}, {1, 3}, {1, 4}]},
//          {{1, 3}, [{1, 2}, {1, 3}]},
//          {{0, 3}, [{0, 3}, {1, 2}, {1, 3}]},
//          {{0, 1}, [{0, 1}, {0, 2}, {0, 3}, {1, 1}, {1, 2}, {1, 3}, {1, 4}]},
//          {{1, 2}, [{1, 2}]},
//          {{0, 2}, [{0, 1}, {0, 2}, {0, 3}, {1, 1}, {1, 2}, {1, 3}]},
//          {{1, 1}, [{0, 1}, {0, 3}, {1, 1}, {1, 2}, {1, 3}]}],
//
//    L3 = [{{2, 2}, [{0, 1}, {2, 2}]},
//          {{2, 3}, [{0, 1}, {1, 1}, {2, 2}, {2, 3}]},
//          {{2, 1}, [{0, 1}, {1, 1}, {2, 1}, {2, 2}, {2, 3}]},
//          {{0, 1}, [{0, 1}]},
//          {{1, 1}, [{0, 1}, {1, 1}, {2, 2}]}],
//
//    L4 = [{{0, 5}, [{0, 5}]},
//          {{0, 4}, [{0, 1}, {0, 3}, {0, 4}, {0, 5}, {0, 6}]},
//          {{0, 1}, [{0, 1}, {0, 3}, {0, 5}]},
//          {{0, 2}, [{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {0, 6}]},
//          {{0, 3}, [{0, 3}, {0, 5}]},
//          {{0, 6}, [{0, 1}, {0, 3}, {0, 5}, {0, 6}]}],
    }

    private void checkTerminationRandomShuffles(Integer nodeNumber, List<CommitDepBox> boxes) {

        int it = 0;
        do {
            // check it terminates
            assertTrue(checkTermination(nodeNumber, boxes));

            // shuffle list
            Collections.shuffle(boxes);

        } while (++it < ITERATIONS);
    }

    private boolean checkTermination(Integer nodeNumber, List<CommitDepBox> boxes) {
        System.out.println("Will add the following list: ");
        for (int i = 0; i < boxes.size(); i++) {
            System.out.println(i + ") " + boxes.get(i));
        }

        DependencyQueue<CommitDepBox> queue = new DependencyQueue<>(nodeNumber);
        List<CommitDepBox> results = new ArrayList<>();
        for (CommitDepBox box : boxes) {
            List<CommitDepBox> result = queue.add(box);
            results.addAll(result);
        }
        return true;
    }

    private CommitDepBox box(Dot dot, HashMap<Integer, ExceptionSet> dep) {
        // build random message
        Message message = Generator.message();

        // create conf, given dep
        HashMap<Integer, MaxInt> conf = new HashMap<>();
        for (Map.Entry<Integer, ExceptionSet> entry : dep.entrySet()) {
            conf.put(entry.getKey(), entry.getValue().toMaxInt());
        }

        return new CommitDepBox(dot, new Clock<>(dep), message, new Clock<>(conf));
    }
}
