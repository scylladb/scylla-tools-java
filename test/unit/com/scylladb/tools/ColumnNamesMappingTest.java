package com.scylladb.tools;

import static com.scylladb.tools.BulkLoader.findFiles;
import static com.scylladb.tools.BulkLoader.openFile;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

public class ColumnNamesMappingTest {
    private static class MockClient implements Client {

        private final Set<String> statements = new HashSet<>();

        @Override
        public CFMetaData getCFMetaData(String keyspace, String cfName) {
            return CFMetaData.Builder.create("loader_test", "loader_test_table")
                    .withPartitioner(new Murmur3Partitioner())
                    .addPartitionKey("new_pk1", Int32Type.instance)
                    .addPartitionKey("new_pk2", Int32Type.instance)
                    .addClusteringColumn("new_ck1", Int32Type.instance)
                    .addClusteringColumn("new_ck2", Int32Type.instance)
                    .addRegularColumn("new_val1", Int32Type.instance)
                    .addRegularColumn("new_val2", Int32Type.instance)
                    .build();
        }

        @Override
        public void processStatment(DecoratedKey key, long timestamp, String what, Map<String, Object> objects) {
            statements.add(what);
        }

        public void assertStatements() {
            Assert.assertTrue(statements.contains("INSERT INTO loader_test.loader_test_table (new_val1,new_pk1,new_pk2,new_ck1,new_ck2) values (?,?,?,?,?) USING TIMESTAMP ?;"));
            Assert.assertTrue(statements.contains("UPDATE loader_test.loader_test_table USING TIMESTAMP ? SET new_val2 = ? WHERE new_pk1 = ? AND new_pk2 = ? AND new_ck1 = ? AND new_ck2 = ?;"));
            Assert.assertTrue(statements.contains("DELETE FROM loader_test.loader_test_table USING TIMESTAMP ? WHERE new_pk1 = ? AND new_pk2 = ?;"));
        }
    }

    @Test
    public void testColumnNamesMapping() throws Exception
    {
        System.setProperty("cassandra.config", "file:///" + new File("test/conf/cassandra.yaml").getAbsolutePath());
        final File dir = new File("test/data/scylla/column_names_mapping");
        final String keyspace = "loader_test";

        final Map<String, String> mapping = new HashMap<>();
        mapping.put("pk1", "new_pk1");
        mapping.put("pk2", "new_pk2");
        mapping.put("ck1", "new_ck1");
        mapping.put("ck2", "new_ck2");
        mapping.put("val1", "new_val1");
        mapping.put("val2", "new_val2");

        MockClient client = new MockClient();
        ColumnNamesMapping columnNamesMapping = new ColumnNamesMapping(mapping);
        for (Pair<Descriptor, Set<Component>> p : findFiles(keyspace, dir)) {
            SSTableReader r = openFile(p, columnNamesMapping.getMetadata(client.getCFMetaData(keyspace, p.left.cfname)));
            if (r == null) {
                continue;
            }
            SStableScannerSource src = new DefaultSSTableScannerSource(r, null);
            SSTableToCQL.Options options = new SSTableToCQL.Options();
            options.columnNamesMapping = columnNamesMapping;
            SSTableToCQL ssTableToCQL = new SSTableToCQL(src);
            ssTableToCQL.run(client, options);
        }
        client.assertStatements();
    }

}
