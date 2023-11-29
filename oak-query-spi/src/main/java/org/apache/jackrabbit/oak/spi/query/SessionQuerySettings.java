package org.apache.jackrabbit.oak.spi.query;

import org.osgi.annotation.versioning.ProviderType;

/**
 * User-specific settings which may be passed by the query engine to index providers during query planning and iteration
 * of results.
 */
@ProviderType
public interface SessionQuerySettings {

    /**
     * Return true to use the index provider's query result count.
     *
     * @return true to use the index provider's query result count
     */
    boolean useDirectResultCount();
}
