package com.scylladb.tools;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;

class SSTableScannerWrapper implements ISSTableScanner {
    private final ISSTableScanner scanner;

    public SSTableScannerWrapper(ISSTableScanner scanner) {
        this.scanner = scanner;
    }

    @Override
    public void close() {
        scanner.close();
    }

    @Override
    public long getLengthInBytes() {
        return scanner.getLengthInBytes();
    }

    @Override
    public long getCompressedLengthInBytes() {
        return scanner.getCompressedLengthInBytes();
    }

    @Override
    public long getCurrentPosition() {
        return scanner.getCurrentPosition();
    }

    @Override
    public long getBytesScanned() {
        return scanner.getBytesScanned();
    }

    @Override
    public String getBackingFiles() {
        return scanner.getBackingFiles();
    }

    @Override
    public boolean hasNext() {
        return scanner.hasNext();
    }

    @Override
    public boolean isForThrift() {
        return scanner.isForThrift();
    }

    @Override
    public CFMetaData metadata() {
        return scanner.metadata();
    }

    @Override
    public UnfilteredRowIterator next() {
        return scanner.next();
    }
}
