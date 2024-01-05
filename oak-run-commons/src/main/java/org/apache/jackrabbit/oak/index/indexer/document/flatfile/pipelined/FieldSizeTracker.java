package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

interface FieldSizeTracker {

    void addField(String name, int size);

    Iterator<Map.Entry<String, Long>> getKnownFields();

    Long getField(String name);


    class NOOP implements FieldSizeTracker {

        @Override
        public void addField(String name, int size) {
        }

        @Override
        public Iterator<Map.Entry<String, Long>> getKnownFields() {
            return Map.<String, Long>of().entrySet().iterator();
        }

        @Override
        public Long getField(String name) {
            return null;
        }
    }

    class HashMapFieldSizeTracker implements FieldSizeTracker {
        private final Map<String, Long> knownFields = new HashMap<>();

        @Override
        public void addField(String name, int size) {
            // If field is not present, add it. Otherwise add the size to the value
            knownFields.merge(name, (long) size, Long::sum);
        }

        @Override
        public Iterator<Map.Entry<String, Long>> getKnownFields() {
            return knownFields.entrySet().iterator();
        }

        @Override
        public Long getField(String name) {
            return knownFields.get(name);
        }
    }
}



