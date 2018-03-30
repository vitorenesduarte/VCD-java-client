package org.imdea.vcd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 *
 * @author Vitor Enes
 */
public class Config {

    @Parameter(names = "-ops")
    private Integer ops = 1000;

    @Parameter(names = "-op")
    private String op = "PUT";

    @Parameter(names = "-clients")
    private Integer clients = 1;

    @Parameter(names = "-conflicts")
    private Boolean conflicts = false;

    @Parameter(names = "-payload_size")
    private Integer payloadSize = 100;

    @Parameter(names = "-node_number")
    private Integer nodeNumber = 1;

    @Parameter(names = "-max_faults")
    private Integer maxFaults;

    @Parameter(names = "-cluster")
    private String cluster = "undefined";

    @Parameter(names = "-timestamp")
    private String timestamp = "undefined";

    @Parameter(names = "-redis")
    private String redis;

    @Parameter(names = "-zk", description = "zk server (host:port)")
    private String zk = "127.0.0.1:2181";

    private Config() {
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

    public Integer getClients() {
        return clients;
    }

    public void setClients(String clients) {
        this.clients = Integer.parseInt(clients);
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

    public static Config parseArgs(String[] args) {
        Config config = new Config();
        config.doParseArgs(args);
        return config;
    }

    public void doParseArgs(String[] args) {
        JCommander jcommander = JCommander.newBuilder()
                .addObject(this)
                .build();

        try {
            jcommander.parse(args);
        } catch (ParameterException e) {
            jcommander.usage();
            throw e;
        }
    }
}
