package org.imdea.vcd;

import org.imdea.vcd.exception.MissingArgumentException;
import org.imdea.vcd.exception.InvalidArgumentException;
import java.util.Arrays;
import java.util.HashSet;

/**
 *
 * @author Vitor Enes
 */
public class Config {

    private static final HashSet<String> REQUIRED = new HashSet<>(Arrays.asList("port"));

    private Integer port;
    private String host;
    private Integer ops;
    private Integer conflictPercentage;
    private String timestamp;
    private String redis;

    private Config() {
        // set config defaults here
        this.host = "localhost";
        this.ops = 1000;
        this.conflictPercentage = 0;
        this.timestamp = null;
        this.redis = null;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = Integer.parseInt(port);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getOps() {
        return ops;
    }

    public void setOps(String ops) {
        this.ops = Integer.parseInt(ops);
    }

    public Integer getConflictPercentage() {
        return conflictPercentage;
    }

    public void setConflictPercentage(String conflictPercentage) {
        this.conflictPercentage = Integer.parseInt(conflictPercentage);
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

    public static Config parseArgs(String[] args) throws InvalidArgumentException, MissingArgumentException {
        Config config = new Config();

        HashSet<String> missing = REQUIRED;

        for (String arg : args) {
            String[] parts = arg.split("=");

            if (parts.length > 2) {
                throw new InvalidArgumentException(arg);
            }

            if (parts.length == 2) {
                // allow empty arguments
                String key = parts[0];
                String value = parts[1];

                switch (key) {
                    case "port":
                        config.setPort(value);
                        break;
                    case "host":
                        config.setHost(value);
                        break;
                    case "ops":
                        config.setOps(value);
                        break;
                    case "conflict_percentage":
                        config.setConflictPercentage(value);
                        break;
                    case "timestamp":
                        config.setTimestamp(value);
                        break;
                    case "redis":
                        config.setRedis(value);
                        break;
                    default:
                        throw new InvalidArgumentException(arg);
                }

                missing.remove(key);
            }
        }

        if (!missing.isEmpty()) {
            throw new MissingArgumentException(missing.iterator().next());
        }

        return config;
    }

}
