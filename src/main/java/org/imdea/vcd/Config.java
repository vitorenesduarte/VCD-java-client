package org.imdea.vcd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 *
 * @author Vitor Enes
 */
@Parameters(separators = "=") // http://jcommander.org/#_parameter_separators
public class Config {

    private final int CLOSED_LOOP = 42;

    @Parameter(names = "-ops")
    private Integer ops = 1000;

    @Parameter(names = "-op")
    private String op = "PUT";

    @Parameter(names = "-clients")
    private Integer clients = 1;

    // if the conflict rate is 142, two classes of clients are created:
    // - one issues non-conflicting commands
    // - the other issues conflicting commands
    @Parameter(names = "-conflicts")
    private Integer conflicts = 100;

    @Parameter(names = "-payload_size")
    private Integer payloadSize = 100;

    @Parameter(names = "-protocol")
    private String protocol = "vcd";

    @Parameter(names = "-node_number")
    private Integer nodeNumber = 1;

    @Parameter(names = "-max_faults")
    private Integer maxFaults = 0;

    @Parameter(names = "-cluster")
    private String cluster = "undefined";

    @Parameter(names = "-timestamp")
    private String timestamp = "undefined";

    @Parameter(names = "-redis")
    private String redis;

    @Parameter(names = "-zk", description = "zk server (host:port)")
    private String zk = "127.0.0.1:2181";

    @Parameter(names = "-sleep")
    private Integer sleep = 340;// if 42, closed loop

    @Parameter(names = "-batch_wait")
    private Integer batchWait = 0; // if 0, batching is disabled

    @Parameter(names = "-opt_delivery", arity = 1)
    private Boolean optDelivery = true;

    private Config() {
    }

    public Integer getOps() {
        return this.ops;
    }

    public void setOps(String ops) {
        this.ops = Integer.parseInt(ops);
    }

    public String getOp() {
        return this.op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Integer getClients() {
        return this.clients;
    }

    public void setClients(String clients) {
        this.clients = Integer.parseInt(clients);
    }

    public Integer getConflicts() {
        return this.conflicts;
    }

    public void setConflicts(String conflicts) {
        this.conflicts = Integer.parseInt(conflicts);
    }

    public Integer getPayloadSize() {
        return this.payloadSize;
    }

    public void setPayloadSize(String payloadSize) {
        this.payloadSize = Integer.parseInt(payloadSize);
    }

    public String getProtocol() {
        return protocol;
    }

    public Integer getNodeNumber() {
        return this.nodeNumber;
    }

    public void setNodeNumber(String nodeNumber) {
        this.nodeNumber = Integer.parseInt(nodeNumber);
    }

    public Integer getMaxFaults() {
        return this.maxFaults;
    }

    public void setMaxFaults(String maxFaults) {
        this.maxFaults = Integer.parseInt(maxFaults);
    }

    public String getCluster() {
        return this.cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getRedis() {
        return this.redis;
    }

    public void setRedis(String redis) {
        this.redis = redis;
    }

    public String getZk() {
        return this.zk;
    }

    public void setZk(String zk) {
        this.zk = zk;
    }

    public Integer getSleep() {
        return this.sleep;
    }

    public boolean getClosedLoop() {
        return this.sleep == CLOSED_LOOP;
    }

    public Integer getBatchWait() {
        return batchWait;
    }

    public void setBatchWait(String batchWait) {
        this.batchWait = Integer.parseInt(batchWait);
    }

    public Boolean getBatching() {
        return batchWait > 0;
    }

    public Boolean getOptDelivery() {
        return this.optDelivery;
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
