package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import java.util.Arrays;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property.ValueType;
import org.junit.Test;

public class PropertyCountTest {
    
    @Test
    public void manyUniqueProperties() {
        PropertyStats pc = new PropertyStats();
        pc.setStorage(new Storage());
        for (int i = 0; i < 1_000_000; i++) {
            Property p = new Property("unique" + i, ValueType.STRING, "");
            pc.add(Arrays.asList(""), Arrays.asList(p));
        }
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 200; j++) {
                Property p = new Property("common" + i, ValueType.STRING, "");
                pc.add(Arrays.asList(""), Arrays.asList(p));
            }
        }
        System.out.println(pc);
    }
}
