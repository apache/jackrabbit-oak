package org.apache.jackrabbit.oak.api;

import java.io.InputStream;

import javax.annotation.Nonnull;

/**
 * Immutable representation of a binary value of finite length.
 */
public interface Blob {

    /**
     * Returns a new stream for this value object. Multiple calls to this
     * methods return equal instances: {@code getNewStream().equals(getNewStream())}.
     * @return a new stream for this value based on an internal conversion.
     */
    @Nonnull
    InputStream getNewStream();

    /**
     * Returns the length of this blob.
     *
     * @return the length of this blob.
     */
    long length();
}
