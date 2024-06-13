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


import java.io.Serializable;
import java.util.Arrays;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.stress.util.ResultLogger;

public class SettingsErrors implements Serializable
{

    public final boolean ignore;
    public final int tries;
    public final boolean skipReadValidation;
    public final boolean skipUnsupportedColumns;

    private enum DelayPolicy {
        CONSTANT,
        LINEAR,
        EXPONENTIAL
    }
    private final DelayPolicy delayPolicy;
    private final long minDelayMs;
    private final long maxDelayMs;

    public SettingsErrors(Options options)
    {
        ignore = options.ignore.setByUser();
        this.tries = Math.max(1, Integer.parseInt(options.retries.value()) + 1);
        skipReadValidation = options.skipReadValidation.setByUser();
        skipUnsupportedColumns = options.skipUnsupportedColumns.setByUser();
        delayPolicy = DelayPolicy.valueOf(options.delayPolicy.value().toUpperCase());
        minDelayMs = Long.parseLong(options.minDelayMs.value());
        maxDelayMs = Long.parseLong(options.maxDelayMs.value());
    }

    // Accessor used by stress.Operation
    public Duration nextDelay(int num_tries) {
        assert num_tries >= 0;
        assert num_tries < tries;
        long delay = minDelayMs;
        switch (delayPolicy) {
        case CONSTANT:
            // wait for constant time between each retry
            break;
        case LINEAR:
            // linearly increase the delay
            delay += (maxDelayMs - minDelayMs) * (float)num_tries / tries;
            break;
        case EXPONENTIAL:
            // exponentially back off
            delay *= (1L << num_tries);
            break;
        default:
            throw new AssertionError();
        }
        // include some jitter to prevent all sessions to retry at the same
        // time.
        double jitter_ratio = ThreadLocalRandom.current().nextDouble(0.75, 1.25);
        delay *= jitter_ratio;
        delay = Math.max(delay, minDelayMs);
        delay = Math.min(delay, maxDelayMs);
        return Duration.ofMillis(delay);
    }

    // Option Declarations
    public static final class Options extends GroupedOptions
    {
        final OptionSimple retries = new OptionSimple("retries=", "[0-9]+", "9", "Number of tries to perform for each operation before failing", false);
        final OptionSimple ignore = new OptionSimple("ignore", "", null, "Do not fail on errors", false);
        final OptionSimple skipReadValidation = new OptionSimple("skip-read-validation", "", null, "Skip read validation and message output", false);
        final OptionSimple skipUnsupportedColumns = new OptionSimple("skip-unsupported-columns", "", null, "Skip unsupported columns, such as maps and embedded collections, when generating data for a user profile.", false);

        final OptionSimple delayPolicy = new OptionSimple("delay-policy=", "constant|linear|exponential", "constant", "Delay before next retry: constant, waits a constant time; linear, increases linearly, exponential, double the delay every time", false);
        final OptionSimple minDelayMs = new OptionSimple("min-delay-ms=", "[0-9]+", "0", "Minimum delay in milliseconds, please use a non-zero value with exponential", false);
        final OptionSimple maxDelayMs = new OptionSimple("max-delay-ms=", "[0-9]+", "20000", "Maximum delay in milliseconds", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(retries, ignore, skipReadValidation, skipUnsupportedColumns,
                                 delayPolicy, minDelayMs, maxDelayMs);
        }

        @Override
        public boolean happy() {
            if (!super.happy()) {
                return false;
            }
            long minDelay = Long.parseLong(minDelayMs.value());
            long maxDelay = Long.parseLong(maxDelayMs.value());
            if (minDelay > maxDelay) {
                return false;
            }
            if (DelayPolicy.valueOf(delayPolicy.value().toUpperCase()) == DelayPolicy.EXPONENTIAL) {
                return minDelay > 0;
            }
            return true;
        }
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Ignore: %b%n", ignore);
        out.printf("  Tries: %d%n", tries);
        if (delayPolicy == DelayPolicy.CONSTANT && minDelayMs == 0) {
            return;
        }
        out.printf("  RetryPolicy: %s%n", delayPolicy.toString().toLowerCase());
        out.printf("  Minimum Delay: %,d %s%n", minDelayMs, TimeUnit.MILLISECONDS.toString());
        if (delayPolicy != DelayPolicy.CONSTANT) {
            out.printf("  Maximum Delay: %,d %s%n", maxDelayMs, TimeUnit.MILLISECONDS.toString());
        }
    }


    public static SettingsErrors get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-errors");
        if (params == null)
            return new SettingsErrors(new Options());

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -errors options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsErrors((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-errors", new Options());
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
