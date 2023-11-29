package org.apache.jackrabbit.oak.query;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.query.QueryCountsSettings;
import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Provides {@link org.apache.jackrabbit.oak.spi.query.QueryCountsSettings} for principals with access to the content
 * repository.
 */
@ProviderType
@FunctionalInterface
public interface QueryCountsSettingsProvider {

    /**
     * Return the applicable {@link org.apache.jackrabbit.oak.spi.query.QueryCountsSettings} for the given session.
     *
     * @param session the subject principal's content session
     * @return the applicable settings
     */
    QueryCountsSettings getQueryCountsSettings(@NotNull ContentSession session);
}
