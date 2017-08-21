package vcd;

import vcd.exception.MissingArgumentException;
import vcd.exception.InvalidArgumentException;
import java.util.Arrays;
import java.util.HashSet;

/**
 *
 * @author Vitor Enes
 */
public class VCDConfig {

    private static final HashSet<String> REQUIRED = new HashSet<>(Arrays.asList("port"));
    
    private Integer port;
    private String host;
    private Integer ops;
    private Integer conflictPercentage;

    private VCDConfig() {
        // set config defaults here
        this.host = "localhost";
        this.ops = 100;
        this.conflictPercentage = 100;
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

    public static VCDConfig parseArgs(String[] args) throws InvalidArgumentException, MissingArgumentException {
        VCDConfig config = new VCDConfig();

        HashSet<String> missing = REQUIRED;
        
        for (String arg : args) {
            String[] parts = arg.split("=");

            if (parts.length != 2) {
                throw new InvalidArgumentException(arg);
            }

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
                default:
                    throw new InvalidArgumentException(arg);
            }

            missing.remove(key);
        }

        if (!missing.isEmpty()) {
            throw new MissingArgumentException(missing.iterator().next());
        }

        return config;
    }

}
