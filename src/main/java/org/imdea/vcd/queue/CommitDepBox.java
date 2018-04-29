package org.imdea.vcd.queue;

import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.MaxInt;
import org.imdea.vcd.pb.Proto;
import org.imdea.vcd.pb.Proto.Commit;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Dots;

/**
 *
 * @author Vitor Enes
 */
public class CommitDepBox implements DepBox<CommitDepBox> {

    private final Dots dots;
    private final Clock<ExceptionSet> dep;
    private final Message message;
    private final Clock<MaxInt> conf;

    public CommitDepBox(Commit commit) {
        this.dots = new Dots(commit.getDot());
        this.dep = Clock.eclock(commit.getDepMap());;
        this.message = commit.getMessage();;
        this.conf = Clock.vclock(commit.getConfMap());;
    }

    @Override
    public boolean before(CommitDepBox box) {
        return box.dep.intersects(this.dots);
    }

    @Override
    public void merge(CommitDepBox box) {
        this.dots.merge(box.dots);
        this.dep.merge(box.dep);
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Proto.MessageSet toMessageSet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
