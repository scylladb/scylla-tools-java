package com.scylladb.tools;

import java.util.Collection;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Ref;

public class DefaultSSTableScannerSource implements SStableScannerSource {
    private Ref<SSTableReader> readerRef;
    private Collection<Range<Token>> range;

    public DefaultSSTableScannerSource(SSTableReader reader, Collection<Range<Token>> range) {
        this.readerRef = reader.ref();
        this.range = range;
    }

    @Override
    public ISSTableScanner scanner() {
        final Ref<SSTableReader> r = readerRef;
        if (r == null) {
            throw new IllegalStateException("Already read");
        }
        readerRef = null;
        SSTableReader reader = r.get();
        ISSTableScanner src = range != null ? reader.getScanner(range, null) : reader.getScanner();

        return new SSTableScannerWrapper(src) {
            @Override
            public void close() {
                super.close();
                r.close();
            }
        };
    }
}
