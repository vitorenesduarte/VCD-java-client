package org.imdea.vcd;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.imdea.vcd.pb.Proto;
import org.imdea.vcd.pb.Proto.MessageSet;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Vitor Enes
 */
public class Socket {

    private final DataRW rw;
    private ZooKeeper zk;

    private Socket(DataRW rw) {
        this.rw = rw;
    }

    public static Socket create(Config config) throws IOException {

        assert config.getZk().split(":").length == 2;

        List<Proto.NodeSpec> nodes = new ArrayList<>();
        String root = "/" + config.getTimestamp();

        try {

            System.out.println("Connecting to: "+config.getZk());

            ZooKeeper zk = new ZooKeeper(
                    config.getZk()+ "/",
                    5000, new ZkWatcher());

            for (String child : zk.getChildren(root, null)) {
                String path = root + "/" + child;
                byte[] data = zk.getData(path, null, null);
                nodes.add(Proto.NodeSpec.parseFrom(data));
            }

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException("Fatal error, cannot connect to Zk.");
        }

        long min = Long.MAX_VALUE;
        Proto.NodeSpec closest = null;
        for (Proto.NodeSpec node : nodes) {
            InetAddress inet = InetAddress.getByName(node.getIp());
            long start = System.currentTimeMillis();
            for (int i=0; i<10; i++)  inet.isReachable(5000);
            long delay = System.currentTimeMillis() - start;
            if (delay < min) {
                min=delay;
                closest = node;
            }
        }
        System.out.println("Closest is "+closest);

        java.net.Socket socket = new java.net.Socket(closest.getIp(), closest.getPort()+1000);
        socket.setTcpNoDelay(true);

        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataRW rw = new DataRW(in, out);

        return new Socket(rw);
    }

    public void send(MessageSet messageSet) throws IOException {
        this.rw.write(messageSet);
    }

    public MessageSet receive() throws IOException {
        return this.rw.read();
    }

    public void closeRw() throws IOException {
        this.rw.close();
    }

    private static class ZkWatcher implements Watcher {

        @Override
        public void process(WatchedEvent watchedEvent) {
        }

    }


}
