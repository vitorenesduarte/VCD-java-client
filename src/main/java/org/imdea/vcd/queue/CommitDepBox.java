package org.imdea.vcd.queue;

import java.util.Map;
import org.imdea.vcd.pb.Proto;

/**
 *
 * @author Vitor Enes
 */
public class CommitDepBox extends DepBox {

    private final Proto.Dot dot;
    private final Proto.Message message;
    private final Map<Integer, Proto.ExceptionSet> dep;
    private final Map<Integer, Integer> conf;
    
    public CommitDepBox(Proto.Commit commit) {
        this.dot = commit.getDot();
        this.message = commit.getMessage();
        this.dep = commit.getDepMap();
        this.conf = commit.getConfMap();
    }

    @Override
    public boolean after(DepBox box) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean before(DepBox box) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void merge(DepBox box) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Proto.MessageSet toMessageSet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
