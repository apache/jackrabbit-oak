package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.IOException;
import java.util.List;

public interface GCJournalFile {

    void writeLine(String line) throws IOException;

    List<String> readLines() throws IOException;

}
