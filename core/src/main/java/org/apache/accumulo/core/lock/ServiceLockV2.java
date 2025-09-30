package org.apache.accumulo.core.lock;


import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A cleaned up representation of the ServiceLock Class
 * This class does not extend the watcher class type of Zookeeper
 *
 */
public class ServiceLockV2 {

    public static final Logger LOG = LoggerFactory.getLogger(ServiceLockV2.class);
    public static final String ZLOCK_PREFIX = "zlock#";

    private final ServiceLockPaths.ServiceLockPath path;
    protected final ZooSession zooKeeper;
    private final String vmLockPrefix;

    private ServiceLock.AccumuloLockWatcher lockWatcher;
    private String lockNodeName;
    private volatile boolean lockWasAcquired;
    private volatile boolean watchingParent;

    private String createdNodeName;
    private String watchingNodeName;

    /**
     * uuid is passed in here to ensure that it is a valid data object coming from another service.
     *
     * @param zookeeper the zookeeper server to communicate with
     * @param path attempted path for lock creation
     * @param uuid sometimes a random or unique value
     */
    public ServiceLockV2(ZooSession zookeeper, ServiceLockPaths.ServiceLockPath path, UUID uuid) {
        this.zooKeeper = requireNonNull(zookeeper);
        this.path = requireNonNull(path);
        this.vmLockPrefix = ZLOCK_PREFIX + uuid.toString() + "#";
    }

    public void acquireLock(Consumer<Void> onLockAcquired) throws Exception {
        lockNodeName = createSeqentialNode();
        verifyLock(onLockAcquired);
    }

    private void verifyLock(Consumer<Void> onLockAcquired) throws Exception {
        List<String> children = zooKeeper.getChildren(path.toString(), null);
        Collections.sort(children);

        String mySequence = createdNodeName.substring(createdNodeName.lastIndexOf('/', + 1));

        if (mySequence.equals(children.get(0))) {
            // Lock Acquired
            onLockAcquired.accept(null);
            return;
        }

        // Lock was not acquired, so create watcher
        int myIndex = children.indexOf(mySequence);
        String predecessor = path.toString() + "/" + children.get(myIndex -1);

        watchPredecessor(predecessor, event -> {
            try {
                verifyLock(onLockAcquired);
            } catch (Exception e) {
                LOG.error("failed to acquire lock", e);
            }
        });
    }

    private void watchPredecessor(String path, Consumer<WatchedEvent> callback) throws Exception {
        zooKeeper.exists(path, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                callback.accept(event);
            }
        });
    }

}
