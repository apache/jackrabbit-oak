package org.apache.jackrabbit.oak.api;

import java.io.InputStream;

import javax.annotation.Nonnull;

public interface Blob {

    /**
     * Returns a new stream for this value object.
     * @return a new stream for this value based on an internal conversion.
     */
    @Nonnull
    InputStream getNewStream();

    /**
     * Returns the length of this blob.
     *
     * @return the length of this bloc.
     */
    long length();
}
