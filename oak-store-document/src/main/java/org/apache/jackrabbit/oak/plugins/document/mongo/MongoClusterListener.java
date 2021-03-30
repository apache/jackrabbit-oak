package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;

public class MongoClusterListener implements ClusterListener {

    private boolean replicaSet = false;
    private boolean connected = false;
    private ServerAddress serverAddress;
    private ServerAddress primaryAddress;

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public ServerAddress getPrimaryAddress() {
        return primaryAddress;
    }

    public boolean isReplicaSet() {
        // Sometimes we need to wait a few seconds in case the connection was just created, the listener
        // didn't have time to receive the description from the cluster.
        try {
            int trials = 30;
            while (!connected && trials > 0) {
                trials--;
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
        }
        return replicaSet;
    }

    @Override
    public void clusterOpening(ClusterOpeningEvent event) {
    }

    @Override
    public void clusterClosed(ClusterClosedEvent event) {
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        for (ServerDescription sd : event.getNewDescription().getServerDescriptions()) {
            if (sd.getState() == ServerConnectionState.CONNECTED) {
                connected = true;
                serverAddress = sd.getAddress();
                primaryAddress = new ServerAddress(sd.getPrimary());
                if (sd.isReplicaSetMember()) {
                    // Can't assign directly the result of the function because in some cases the cluster
                    // type is UNKNOWN, mainly when the cluster is changing it's PRIMARY.
                    replicaSet = true;
                }
            }
        }
    }
}
