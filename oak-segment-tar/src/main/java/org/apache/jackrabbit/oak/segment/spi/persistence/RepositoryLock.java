package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.IOException;

public interface RepositoryLock {

    void unlock() throws IOException;

}
