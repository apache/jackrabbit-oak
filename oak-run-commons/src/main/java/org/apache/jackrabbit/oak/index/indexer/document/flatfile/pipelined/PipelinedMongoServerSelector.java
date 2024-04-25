package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.selector.ServerSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Selects a Mongo server that is available for a new connection. This policy is used by PipelinedMongoDownloadTask,
 * the goal being to spread the two connections between the secondaries and avoid downloading from the primary.
 * The policy tries to ensure the following:
 *
 * <p>
 * - Establish new connections only to secondaries.
 * - Do not establish more than one connection to a secondary.
 * - If there is a connection to a secondary and that secondary is promoted to primary, the thread should disconnect
 * from the primary and reconnect to a secondary.
 * <p>
 * This class uses the thread id of the caller to distribute the connections, that is, it assumes that there are
 * two threads downloading from Mongo and each thread should be sent to a different secondary. If a thread calls several
 * times the selection logic, it will receive always the same server. The thread id is used to identify the calling
 * thread.
 */
public class PipelinedMongoServerSelector implements ServerSelector, ClusterListener {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoServerSelector.class);

    // Thread ID -> ServerAddress
    private final HashMap<Long, ServerAddress> serverAddressHashMap = new HashMap<>();
    // IDs of threads connected to primary
    private final HashSet<Long> connectedToPrimaryThreads = new HashSet<>();

    private ClusterDescription lastSeenClusterDescription;

    @Override
    synchronized public List<ServerDescription> select(ClusterDescription clusterDescription) {
        return select(clusterDescription.getType(), clusterDescription.getServerDescriptions());
    }

    // Exposed for testing
    List<ServerDescription> select(ClusterType clusterType, List<ServerDescription> serverDescriptions) {
        if (clusterType != ClusterType.REPLICA_SET) {
            LOG.info("Cluster is not a replica set, returning all servers");
            return serverDescriptions;
        }
        String threadName = Thread.currentThread().getName();
        if (!threadName.startsWith(PipelinedMongoDownloadTask.THREAD_NAME_PREFIX + "-")) {
            LOG.info("Thread name does not start with {}, returning all servers", PipelinedMongoDownloadTask.THREAD_NAME_PREFIX);
            return serverDescriptions;
        }

        Long threadId = Thread.currentThread().getId();
        LOG.debug("Selecting server from cluster: {}", serverDescriptions.stream()
                .map(sd -> sd.getAddress() + ", " + sd.getType() + ", " + sd.getState())
                .collect(Collectors.joining("\n", "\n", "")));
        var secondaries = serverDescriptions.stream()
                .filter(ServerDescription::isSecondary)
                .collect(Collectors.toSet());
        if (secondaries.isEmpty()) {
            LOG.info("No secondaries found, not selecting the primary. Returning empty list.");
            return List.of();
        }
        // Secondaries available
        serverAddressHashMap.remove(threadId);
        // No server assigned to this thread yet. Find an unused server
        for (var replica : secondaries) {
            var address = replica.getAddress();
            if (!serverAddressHashMap.containsValue(address)) {
                // This server is not assigned to any thread yet. Assign it to this thread
                serverAddressHashMap.put(threadId, address);
                LOG.info("Selected server: {}", address);
                return List.of(replica);
            }
        }
        // All secondaries are already assigned to some thread
        LOG.debug("All available Mongo secondaries are assigned. Returning empty list.");
        return List.of();
    }

    @Override
    public void clusterOpening(ClusterOpeningEvent event) {
        // Ignore
    }

    @Override
    public synchronized void clusterClosed(ClusterClosedEvent event) {
        // Ignore
    }

    @Override
    public synchronized void clusterDescriptionChanged(ClusterDescriptionChangedEvent event) {
        // Secondaries can be promoted to primary at any time. If that happens, any downloader thread connected to this
        // replica should disconnect. We use the cluster description event to detect if any of the threads is connected
        // to the replica that is now the primary. The downloader threads are then responsible for periodically calling
        // #isConnectedToPrimary to check if they are connected to the primary and in that case close the connection
        // and open a new connection, which will trigger the Mongo driver to call again the selection logic in this class
        this.lastSeenClusterDescription = event.getNewDescription();
        if (lastSeenClusterDescription.getType() != ClusterType.REPLICA_SET) {
            return;
        }

        // Check if any thread is connected to primary
        connectedToPrimaryThreads.clear();
        lastSeenClusterDescription.getServerDescriptions().stream()
                .filter(ServerDescription::isPrimary)
                .map(ServerDescription::getAddress)
                .forEach(primaryAddress -> {
                    for (var entry : serverAddressHashMap.entrySet()) {
                        if (entry.getValue().equals(primaryAddress)) {
                            connectedToPrimaryThreads.add(entry.getKey());
                        }
                    }
                });
    }

    /**
     * Returns true if the current thread is connected to the primary.
     */
    public synchronized boolean isConnectedToPrimary() {
        return connectedToPrimaryThreads.contains(Thread.currentThread().getId());
    }

    /**
     * Returns true if there is at least one connection active.
     */
    public synchronized boolean atLeastOneConnectionActive() {
        return !serverAddressHashMap.isEmpty();
    }

    /**
     * Called by the downloader thread when it finishes downloading. This method removes the thread from the list of
     * active threads.
     */
    public synchronized void threadFinished() {
        LOG.info("Thread finished downloading. Unregistering.");
        var previous = serverAddressHashMap.remove(Thread.currentThread().getId());
        if (lastSeenClusterDescription.getType() == ClusterType.REPLICA_SET && previous == null) {
            LOG.warn("Thread was not registered with Mongo server selector.");
        }
    }
}
