package org.imdea.vcd;

/**
 *
 * @author Vitor Enes
 */
public class Experiment {

    public static void main(String[] args) {
            Config config = Config.parseArgs(args);
            if(config.getClosedLoop()) {
                ClosedLoopClient.run(config);
            } else {
                OpenLoopClient.run(config);
            }
    }
}