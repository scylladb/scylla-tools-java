package com.scylladb.tools;

import static com.scylladb.tools.LocalFileKeyGenerator.COM_SCYLLADB_SYSTEM_KEY_DIR;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.lines;
import static java.nio.file.Files.readAllLines;
import static junit.framework.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EncryptionToolsTest {
    private Path tempDir;

    @Before
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("encr").toAbsolutePath();
    }

    @After
    public void tearDown() throws IOException {
        if (tempDir != null) {
            Files.walkFileTree(tempDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return super.visitFile(file, attrs);
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return super.postVisitDirectory(dir, exc);
                }
            });
        }
        tempDir = null;
    }

    @Test
    public void testCreateKeyfile() throws IOException {
        Path file = tempDir.resolve("apa_key");
        LocalFileKeyGenerator.main(file.toString());

        assertTrue(exists(file));
        assertTrue(lines(file).count() > 0);
    }

    @Test
    public void testCreateKeyfileWithSystemKeyDir() throws IOException {
        Path file = tempDir.resolve("apa_key");
        try {
            System.getProperties().setProperty(COM_SCYLLADB_SYSTEM_KEY_DIR, tempDir.toString());
            LocalFileKeyGenerator.main("apa_key");

            assertTrue(exists(file));
            assertTrue(lines(file).count() > 0);
        } finally {
            System.getProperties().remove(COM_SCYLLADB_SYSTEM_KEY_DIR);
        }
    }

    @Test
    public void testCreateKeyfileWithLength() throws IOException {
        Path file = tempDir.resolve("apa_key");
        LocalFileKeyGenerator.main("-length", "256", file.toString());

        assertTrue(exists(file));
        assertTrue(lines(file).count() > 0);
        assertTrue(readAllLines(file).get(0).indexOf(":256:") != -1);
    }

    @Test
    public void testConfigEncryption() throws IOException {
        Path key = tempDir.resolve("apa_key");
        String contents = configEncryptionHelper(key);
        // should have the full key path in it
        assertTrue(contents.indexOf(key.toString()) != -1);
    }

    @Test
    public void testConfigEncryptionWithSystemKeyDir() throws IOException {

        try {
            System.getProperties().setProperty(COM_SCYLLADB_SYSTEM_KEY_DIR, tempDir.toString());
            Path key = Paths.get("apa_key");
            String contents = configEncryptionHelper(key);
            // should have key as relative path
            assertTrue(contents.indexOf(tempDir.toString()) == -1);
        } finally {
            System.getProperties().remove(COM_SCYLLADB_SYSTEM_KEY_DIR);
        }
    }

    private String configEncryptionHelper(Path key) throws IOException {
        testCreateKeyfile();
        Path config = tempDir.resolve("apa.yaml");
        Path encrypted = tempDir.resolve("apa_enc.yaml");
        Path decrypted = tempDir.resolve("apa_dec.yaml");

        try (BufferedWriter w = Files.newBufferedWriter(config)) {
            // fake partial scylla.conf
            w.write("# Scylla storage config YAML\n" + "\n" + "#######################################\n"
                    + "# This file is split to two sections:\n" + "# 1. Supported parameters\n"
                    + "# 2. Unsupported parameters: reserved for future use or backwards\n" + "#    compatibility.\n"
                    + "# Scylla will only read and use the first segment\n"
                    + "#######################################\n" + "\n" + "### Supported Parameters\n" + "\n"
                    + "# The name of the cluster. This is mainly used to prevent machines in\n"
                    + "# one logical cluster from joining another.\n"
                    + "# It is recommended to change the default value when creating a new cluster.\n"
                    + "# You can NOT modify this value for an existing cluster\n" + "#cluster_name: 'Test Cluster'\n"
                    + "# The size of the individual commitlog file segments.  A commitlog\n"
                    + "# segment may be archived, deleted, or recycled once all the data\n"
                    + "# in it (potentially from each columnfamily in the system) has been\n"
                    + "# flushed to sstables.\n" + "#\n"
                    + "# The default size is 32, which is almost always fine, but if you are\n"
                    + "# archiving commitlog segments (see commitlog_archiving.properties),\n"
                    + "# then you probably want a finer granularity of archiving; 8 or 16 MB\n" + "# is reasonable.\n"
                    + "commitlog_segment_size_in_mb: 32\n" + "" + "#KMIP\n" + "kmip_hosts:\n" + "    kmip_test:\n"
                    + "        hosts: kmip-interop1.cryptsoft.com\n"
                    + "        certificate: /home/calle/src/kmipc-1.9.1c/SCYLLADB.pem\n"
                    + "        keyfile: /home/calle/src/kmipc-1.9.1c/SCYLLADB.pem\n"
                    + "        truststore: /home/calle/src/kmipc-1.9.1c/CA.pem\n" + "        username: arne\n"
                    + "        password: nils\n");

        }

        ConfigEncryptor.main("-c", config.toString(), "-o", encrypted.toString(), key.toString());

        assertTrue(exists(encrypted));
        assertTrue(lines(encrypted).count() > 0);

        String encContents = new String(Files.readAllBytes(encrypted), "UTF-8");

        assertTrue(encContents.indexOf("# flushed to sstables.") != -1);
        assertTrue(encContents.indexOf("nils") == -1);

        // decrypt again

        ConfigEncryptor.main("-d", "-c", encrypted.toString(), "-o", decrypted.toString());

        assertTrue(exists(decrypted));
        assertTrue(lines(decrypted).count() > 0);

        String decContents = new String(Files.readAllBytes(decrypted), "UTF-8");

        assertTrue(decContents.indexOf("# flushed to sstables.") != -1);
        assertTrue(decContents.indexOf("nils") != -1);

        return encContents;
    }
}
