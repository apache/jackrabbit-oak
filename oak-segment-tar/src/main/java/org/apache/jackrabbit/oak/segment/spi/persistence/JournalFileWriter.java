package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.Closeable;
import java.io.IOException;

public interface JournalFileWriter extends Closeable {

    void truncate() throws IOException;

    void writeLine(String line) throws IOException;

}
