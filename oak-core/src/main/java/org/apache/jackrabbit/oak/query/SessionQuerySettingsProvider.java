package org.apache.jackrabbit.oak.query;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.query.SessionQuerySettings;
import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Provides {@link org.apache.jackrabbit.oak.spi.query.SessionQuerySettings} for principals with access to the content
 * repository.
 */
@ProviderType
@FunctionalInterface
public interface SessionQuerySettingsProvider {

    /**
     * Return the applicable {@link org.apache.jackrabbit.oak.spi.query.SessionQuerySettings} for the given session.
     *
     * @param session the subject principal's content session
     * @return the applicable query settings
     */
    SessionQuerySettings getQuerySettings(@NotNull ContentSession session);
}
