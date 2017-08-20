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

    private VCDConfig() {
        // set config defaults here
        this.host = "localhost";
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
