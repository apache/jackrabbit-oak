package org.apache.jackrabbit.oak.api.blob;

import javax.jcr.RepositoryException;

public class UnsupportedHttpUploadArgumentsException extends RepositoryException {
    public UnsupportedHttpUploadArgumentsException() {
        super();
    }
    public UnsupportedHttpUploadArgumentsException(final String message) {
        super(message);
    }
    public UnsupportedHttpUploadArgumentsException(final Exception e) {
        super(e);
    }
}
