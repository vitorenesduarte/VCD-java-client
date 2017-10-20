package org.imdea.vcd;

import org.imdea.vcd.exception.InvalidArgumentException;
import org.imdea.vcd.exception.MissingArgumentException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
/**
 *
 * @author Vitor Enes
 */
public class Config {

    @Option(name = "-zk", usage = "zk server (host:port)")
    private String zk = "127.0.0.1:2181";

    @Option(name = "-clients")
    private Integer clients = 1;

    @Option(name = "-ops")
    private Integer ops = 1000;

    @Option(name = "-op")
    private String op = "PUT";

    @Option(name = "-conflicts")
    private Boolean conflicts = false;

    @Option(name = "-payload_size")
    private Integer payloadSize = 100;

    @Option(name = "-node_number")
    private Integer nodeNumber = 1;

    @Option(name = "-max_faults")
    private Integer maxFaults ;

    @Option(name = "-cluster")
    private String cluster;

    @Option(name = "-timestamp")
    private String timestamp = "undefined";

    @Option(name = "-redis")
    private String redis;

    private Config() {
    }

    public Integer getClients() {
        return clients;
    }

    public void setClients(String clients) {
        this.clients = Integer.parseInt(clients);
    }

    public Integer getOps() {
        return ops;
    }

    public void setOps(String ops) {
        this.ops = Integer.parseInt(ops);
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Boolean getConflicts() {
        return conflicts;
    }

    public void setConflicts(String conflicts) {
        this.conflicts = Boolean.parseBoolean(conflicts);
    }

    public Integer getPayloadSize() {
        return payloadSize;
    }

    public void setPayloadSize(String payloadSize) {
        this.payloadSize = Integer.parseInt(payloadSize);
    }

    public Integer getNodeNumber() {
        return nodeNumber;
    }

    public void setNodeNumber(String nodeNumber) {
        this.nodeNumber = Integer.parseInt(nodeNumber);
    }

    public Integer getMaxFaults() {
        return maxFaults;
    }

    public void setMaxFaults(String maxFaults) {
        this.maxFaults = Integer.parseInt(maxFaults);
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getRedis() {
        return redis;
    }

    public void setRedis(String redis) {
        this.redis = redis;
    }

    public String getZk() {
        return zk;
    }

    public void setZk(String zk) {
        this.zk = zk;
    }

    public static Config parseArgs(String[] args) throws InvalidArgumentException, MissingArgumentException {
        Config config = new Config();
        config.doParseArgs(args);
        return config;
    }

    public void doParseArgs(String[] args) {

        CmdLineParser parser = new CmdLineParser(this);

        parser.setUsageWidth(80);

        try {
            parser.parseArgument(args);

        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.err.println();
        }

    }

}
