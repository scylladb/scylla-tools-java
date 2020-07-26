/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

package com.scylladb.tools;

import static java.lang.Integer.parseInt;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.util.EnumSet.of;
import static javax.crypto.KeyGenerator.getInstance;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Base64;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.annotations.VisibleForTesting;

public class LocalFileKeyGenerator {
    @VisibleForTesting
    public static final String COM_SCYLLADB_SYSTEM_KEY_DIR = "com.scylladb.SystemKeyDir";
    @VisibleForTesting
    public static final String TOOL_NAME = "local_file_key_generator";
    @VisibleForTesting
    public static final String DEFAULT_SYSTEM_KEY_DIR = System.getProperty(COM_SCYLLADB_SYSTEM_KEY_DIR,
            "resources/system_keys");

    private static void printUsage(Options options) {
        String usage = String.format("%s [options] [key path|key name]", TOOL_NAME);
        String header = System.lineSeparator()
                + "Generate a local file (system) key at <key path> with the provided length and algorithm.";
        String footer = System.lineSeparator() + "(Requires Java cryptographic extensions)";
        new HelpFormatter().printHelp(usage, header, options, footer);
    }

    public static void main(String... args) {
        Options options = new Options();

        options.addOption("a", "alg", true, "Key algorithm (i.e. AES, 3DES)");
        options.addOption("m", "block-mode", true, "Algorithm block mode (i.e. CBC, EBC)");
        options.addOption("p", "padding", true, "Algorithm padding method (i.e. PKCS5)");
        options.addOption("l", "length", true, "Key length in bits (i.e. 128, 256)");
        options.addOption("c", "append", false, "Append to key file (default is to overwrite)");
        options.addOption("h", "help", false, "print help");

        CommandLineParser parser = new GnuParser();
        try {
            CommandLine cmd = parser.parse(options, args, false);

            if (cmd.hasOption("help")) {
                printUsage(options);
                System.exit(0);
            }

            String[] files = cmd.getArgs();

            if (files.length > 1) {
                throw new ConfigurationException("Too many arguments");
            }

            String alg = cmd.getOptionValue("alg", "AES").toUpperCase();
            String mode = cmd.getOptionValue("block-mode", "CBC").toUpperCase();
            String padd = cmd.getOptionValue("padding", "PKCS5").toUpperCase();
            int len = parseInt(cmd.getOptionValue("length", "128"));
            boolean append = cmd.hasOption("append");

            if (!padd.endsWith("Padding")) {
                padd = padd + "Padding";
            }

            KeyGenerator generator = getInstance(alg);
            generator.init(len);
            SecretKey key = generator.generateKey();

            String keyString = Base64.getEncoder().encodeToString(key.getEncoded());

            String keyName = "system_key";

            String line = alg + "/" + mode + "/" + padd + ":" + len + ":" + keyString;

            if (files.length > 0) {
                File f = new File(files[0]);
                if (f.isDirectory()) {
                    f = new File(f, keyName);
                }
                if (!f.exists()) {
                    if (f.getParentFile() != null) {
                        f.getParentFile().mkdirs();
                    }
                    Files.createFile(f.toPath(), asFileAttribute(of(OWNER_READ, OWNER_WRITE)));
                }

                try (PrintWriter w = new PrintWriter(new FileWriter(f, append))) {
                    w.println(line);
                    w.flush();
                }
            } else {
                System.out.println(line);
            }
        } catch (ParseException | ConfigurationException e) {
            System.err.println(e.getMessage());
            printUsage(options);
            System.exit(1);
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }
    }

    static String getSystemKeyDir() {
        return System.getProperty(COM_SCYLLADB_SYSTEM_KEY_DIR, DEFAULT_SYSTEM_KEY_DIR);
    }
}
