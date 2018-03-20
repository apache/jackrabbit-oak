package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.IOException;
import java.util.Properties;

public interface ManifestFile {

    boolean exists();

    Properties load() throws IOException;

    void save(Properties properties) throws IOException;

}
