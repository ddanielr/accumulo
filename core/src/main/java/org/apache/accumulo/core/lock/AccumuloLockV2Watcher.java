package org.apache.accumulo.core.lock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;


public class AccumuloLockV2Watcher implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(AccumuloLockV2Watcher.class);
    Consumer<ServiceLock> lostLockAction;

    public AccumuloLockV2Watcher(Consumer<ServiceLock> serviceLockConsumer) {
        this.lostLockAction = serviceLockConsumer;
    }

    // Need to have the ability to pass function calls down to this.

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("{}", watchedEvent);
        }

        // Only referenced in lockobject
        watchingParent = false;

        if (watchedEvent.getState() == Event.KeeperState.Expired && lockNodeName != null) {
            lostLock(ServiceLock.LockLossReason.SESSION_EXPIRED);
        } else {

            try { // set the watch on the parent node again
                zooKeeper.exists(path.toString(), this);
                watchingParent = true;
            } catch (KeeperException.ConnectionLossException ex) {
                // we can't look at the lock because we aren't connected, but our session is still good
                LOG.warn("lost connection to zookeeper", ex);
            } catch (Exception ex) {
                if (lockNodeName != null || createdNodeName != null) {
                    lockWatcher.unableToMonitorLockNode(ex);
                    LOG.error("Error resetting watch on ZooLock {} {}",
                            lockNodeName != null ? lockNodeName : createdNodeName, event, ex);
                }
            }

        }

    }

    }
}
