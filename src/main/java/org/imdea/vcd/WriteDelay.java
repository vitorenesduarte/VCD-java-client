package org.imdea.vcd;

import java.util.concurrent.ConcurrentHashMap;
import org.imdea.vcd.pb.Proto.Init;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.MaxInt;

/**
 *
 * @author Vitor Enes
 */
public class WriteDelay {

    private Integer site;
    private ExceptionSet committed;
    private ConcurrentHashMap<Dot, Long> ndd; // non-delivered deps

    private final Object monitor = new Object();

    public void init(Init init) {
        this.site = init.getSite();
        Clock<ExceptionSet> committedClock = Clock.eclock(init.getCommittedMap());
        this.committed = (ExceptionSet) committedClock.get(this.site);
        this.ndd = new ConcurrentHashMap<>();
        monitorNotify();
    }

    public void commit(Dot dot, Clock<MaxInt> conf) {
        if (dot.getId().equals(this.site)) {
            this.committed.add(dot.getSeq());
        }
        this.ndd.put(dot, conf.get(this.site).current());
    }

    public void deliver(Dot dot) {
        this.ndd.remove(dot);
        monitorNotify();
    }

    public void waitDepsCommitted() throws InterruptedException {
        // wait until it's initialized
        if (this.ndd == null) {
            monitorWait();
            waitDepsCommitted();
        }
        // wait all deps are committed
        boolean allCommitted = true;
        for (Long dep : this.ndd.values()) {
            allCommitted = allCommitted && this.committed.containsAll(dep);
        }
        if (!allCommitted) {
            monitorWait();
            waitDepsCommitted();
        }
    }

    private void monitorNotify() {
        synchronized (this.monitor) {
            this.monitor.notify();
        }
    }

    private void monitorWait() throws InterruptedException {
        synchronized (this.monitor) {
            this.monitor.wait();
        }
    }
}
