package org.imdea.vcd.queue;

import org.imdea.vcd.pb.Proto;
import org.imdea.vcd.pb.Proto.Commit;
import org.imdea.vcd.pb.Proto.Dot;
import org.imdea.vcd.pb.Proto.Message;

/**
 *
 * @author Vitor Enes
 */
public class CommitDepBox extends DepBox {

    private final Message message;
    private final VClock conf;
    
    public static CommitDepBox fromCommit(Commit commit) {
        Dot dot = commit.getDot();
        EClock dep = new EClock(commit.getDepMap());
        Message message = commit.getMessage();
        VClock conf = new VClock(commit.getConfMap());
        return new CommitDepBox(dot, dep, message, conf);
    }

    public CommitDepBox(Dot dot, EClock dep, Message message, VClock conf) {
        super(dot, dep);
        this.message = message;
        this.conf = conf;
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
  
