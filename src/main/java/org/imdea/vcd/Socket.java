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
import java.util.concurrent.Semaphore;
import static org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected;

/**
 *
 * @author Vitor Enes
 */
public class Socket {

    private final DataRW rw;

    protected Socket(DataRW rw) {
        this.rw = rw;
    }

    public static Socket create(Config config) throws IOException {

        Proto.NodeSpec closest = getClosestNode(config);
        System.out.println("Closest node is " + closest);

        java.net.Socket socket = new java.net.Socket(closest.getIp(), closest.getPort() + 1000);
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

    private static Proto.NodeSpec getClosestNode(Config config) throws IOException {
        List<Proto.NodeSpec> nodes = getAllNodes(config);

        long min = Long.MAX_VALUE;
        Proto.NodeSpec closest = null;

        for (Proto.NodeSpec node : nodes) {
            InetAddress inet = InetAddress.getByName(node.getIp());
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                inet.isReachable(5000);
            }
            long delay = System.currentTimeMillis() - start;
            if (delay < min) {
                min = delay;
                closest = node;
            }
        }

        return closest;
    }
    
    private static List<Proto.NodeSpec> getAllNodes(Config config) throws IOException {
        List<Proto.NodeSpec> nodes = new ArrayList<>();
        String root = "/" + config.getTimestamp();

        try {
            ZooKeeper zk = zkConnection(config);

            for (String child : zk.getChildren(root, null)) {
                String path = root + "/" + child;
                byte[] data = zk.getData(path, null, null);
                nodes.add(Proto.NodeSpec.parseFrom(data));
            }
            zk.close();

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace(System.err);
            throw new RuntimeException("Fatal error, cannot connect to Zk.");
        }
        
        return nodes;
    }
    
    /**
     * Since connecting to ZooKeeper is asynchronous,
     * this method only returns once the watcher is notified
     * the connection is ready.
     */
    private static ZooKeeper zkConnection(Config config) throws IOException, InterruptedException {
        assert config.getZk().split(":").length == 2;
        
        Semaphore semaphore = new Semaphore(0);
        ZooKeeper zk = new ZooKeeper(
                    config.getZk() + "/",
                    5000, new ZkWatcher(semaphore));
        semaphore.acquire();
        
        return zk;
    }

    private static class ZkWatcher implements Watcher {
        
        private final Semaphore semaphore;
        
        public ZkWatcher(Semaphore semaphore) {
            this.semaphore = semaphore;
        }

        @Override
        public void process(WatchedEvent watchedEvent) {
            switch(watchedEvent.getState()) {
                case SyncConnected:
                    semaphore.release();
                    break;
            }
        }

    }
}
