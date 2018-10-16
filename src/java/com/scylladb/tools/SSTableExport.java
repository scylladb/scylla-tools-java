package com.scylladb.tools;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.tools.Util;

public class SSTableExport extends org.apache.cassandra.tools.SSTableExport {
    private boolean legacy = true;

    @Override
    protected CFMetaData metadataFromSSTable(Descriptor desc) throws IOException {
        if (!legacy && desc.version.storeRows()) {
            return super.metadataFromSSTable(desc);
        }

        DatabaseDescriptor.clientInitialization(false);
        Util.initDatabaseDescriptor();
        // load keyspace descriptions.
        Schema.instance.loadFromDiskForTool();

        CFMetaData metaData = Schema.instance.getCFMetaData(desc);
        if (metaData == null) {
            throw new IllegalArgumentException(String.format(
                    "Could not locate schema info for %s/(%s:%s). "
                            + "Make sure your 'data_file_directories' is pointed correctly.",
                    desc, desc.ksname, desc.cfname));
        }
        return metaData;
    }

    @Override
    protected void checkValidDescriptor(File file) {
        legacy = Descriptor.isLegacyFile(file);
    }

    public static void main(String args[]) throws ConfigurationException {
        new SSTableExport().run(args);
    }
}
