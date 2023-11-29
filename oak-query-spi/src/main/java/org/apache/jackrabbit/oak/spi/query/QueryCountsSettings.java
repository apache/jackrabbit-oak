package org.apache.jackrabbit.oak.spi.query;

import org.osgi.annotation.versioning.ProviderType;

/**
 * Privileged settings which are passed by the query engine to index providers during query planning and iteration
 * of results.
 */
@ProviderType
public interface QueryCountsSettings {

    /**
     * Return true to use the index provider's query result count.
     *
     * @return true to use the index provider's query result count
     */
    boolean useDirectResultCount();
}
