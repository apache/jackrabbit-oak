package org.apache.jackrabbit.oak.plugins.memory;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Optional;
import org.apache.jackrabbit.oak.api.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for {@link Blob} implementations.
 * This base class provides default implementations for
 * {@code hashCode} and {@code equals}.
 */
public abstract class AbstractBlob implements Blob {
    private static final Logger log = LoggerFactory.getLogger(AbstractBlob.class);

    private Optional<Integer> hashCode = Optional.absent();

    /**
     * This hash code implementation returns the hash code of the underlying stream
     * @return
     */
    @Override
    public int hashCode() {
        // Blobs are immutable so we can safely cache the hash
        if (!hashCode.isPresent()) {
            InputStream s = getNewStream();
            try {
                hashCode = Optional.of(s.hashCode());
            }
            finally {
                close(s);
            }
        }
        return hashCode.get();
    }

    /**
     * To {@code Blob} instances are considered equal iff they have the same hash code
     * are equal.
     * @param other
     * @return
     */
    @Override
    public boolean equals(Object other) {
        return other == this || other instanceof Blob && hashCode() == other.hashCode();
    }

    private static void close(InputStream s) {
        try {
            s.close();
        }
        catch (IOException e) {
            log.warn("Error while closing stream", e);
        }
    }
}
