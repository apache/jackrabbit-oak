package org.apache.jackrabbit.oak.plugins.blob;

import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;

/**
 */
public interface DataStoreCacheStatsMBean extends CacheStatsMBean {
    /**
     * Total weight of the in-memory cache
     * @return to weight of the cache
     */
    //Computing weight is costly hence its an operation
    long estimateCurrentMemoryWeight();
}
