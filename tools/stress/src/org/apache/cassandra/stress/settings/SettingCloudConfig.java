package org.apache.cassandra.stress.settings;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.util.ResultLogger;

public class SettingCloudConfig implements Serializable
{
    public final static String[] FORBIDDENS = {"-node", "-port", "-transport"};
    public final File file;

    public SettingCloudConfig(Options options)
    {
        if (options.INPUT_FILE.value() == null)
        {
            this.file = null;
        }
        else
        {
            this.file = new File(options.INPUT_FILE.value());
        }
    }

    // Option Declarations

    public static final class Options extends GroupedOptions
    {

        private static final OptionSimple INPUT_FILE = new OptionSimple("file=", ".*\\.yaml", null, "Scylla Cloud .yaml configuration file", true);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(INPUT_FILE);
        }
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  File: %s%n", file);
    }


    public static SettingCloudConfig get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-cloudconf");
        if (params == null)
            return new SettingCloudConfig(new Options());

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -cloudconf options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingCloudConfig((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-cloudconf", new Options());
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }
}
