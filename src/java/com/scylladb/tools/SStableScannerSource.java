package com.scylladb.tools;

import org.apache.cassandra.io.sstable.ISSTableScanner;

public interface SStableScannerSource {
    ISSTableScanner scanner();
}
