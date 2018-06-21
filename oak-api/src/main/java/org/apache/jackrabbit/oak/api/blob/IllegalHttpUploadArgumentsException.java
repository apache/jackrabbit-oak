package org.apache.jackrabbit.oak.api.blob;

import javax.jcr.RepositoryException;

public class IllegalHttpUploadArgumentsException extends RepositoryException {
    public IllegalHttpUploadArgumentsException() {
        super();
    }
    public IllegalHttpUploadArgumentsException(final String message) {
        super(message);
    }
    public IllegalHttpUploadArgumentsException(final Exception e) {
        super(e);
    }
}
