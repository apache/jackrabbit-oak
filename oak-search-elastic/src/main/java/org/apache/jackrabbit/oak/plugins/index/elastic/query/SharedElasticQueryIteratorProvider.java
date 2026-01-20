package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

/**
 * Encapsulates and iterator and keeps its entries in memory and allowing to generate multiple iterators
 * over the same data. Fetching from the original iterator is done on-demand.
 */
public class SharedElasticQueryIteratorProvider {
    private final ElasticQueryIterator delegate;

    private final ArrayList<FulltextIndex.FulltextResultRow> data = new ArrayList<>();
    private volatile boolean saturated = false;
    private volatile long size = 0;
    private Throwable error;

    /**
     * Creates an iterator provider allowing to access multiple times the same data from an original iterator.
     * @param elasticQueryIterator the original iterator to allow multi-access to
     */
    public SharedElasticQueryIteratorProvider(@NotNull ElasticQueryIterator elasticQueryIterator) {
        this.delegate = elasticQueryIterator;
    }

    private boolean haveIndex(int index) {
        if(index < size) {
            return true;
        }
        if(saturated) {
            return false;
        }
        while(!saturated && index >= size && error == null) {
            tryFetchingMore(index);
        }
        if(error != null) {
            throw new RuntimeException(error);
        }
        return index < size;
    }

    private void tryFetchingMore(int limitIndex) {
        synchronized (delegate) {
            if(saturated || limitIndex < size || error != null) {
                return;
            }
            try {
                if (delegate.hasNext()) {
                    data.add(delegate.next());
                    size++;
                } else {
                    saturated = true;
                }
            } catch (Throwable t) {
                error = t;
            }
        }
    }

    /**
     * @return An iterator over the original data
     */
    public ElasticQueryIterator getIterator() {
        return new ElasticQueryIterator() {
            private int index;
            @Override
            public boolean hasNext() {
                return haveIndex(index);
            }

            @Override
            public FulltextIndex.FulltextResultRow next() {
                return data.get(index++);
            }

            @Override
            public String explain() {
                return delegate.explain();
            }
        };
    }
}
