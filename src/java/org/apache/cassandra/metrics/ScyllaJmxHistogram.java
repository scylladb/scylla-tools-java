package org.apache.cassandra.metrics;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Arrays;

import static java.lang.Math.floor;

public class ScyllaJmxHistogram implements CassandraMetricsRegistry.JmxHistogramMBean,CassandraMetricsRegistry.MetricMBean
    {

        private ObjectName objectName;

        @Override
        public ObjectName objectName()
        {
            return objectName;
        }

        public ScyllaJmxHistogram() {
            histogram = new Histogram();
            try {
                objectName=new ObjectName("");
            } catch (MalformedObjectNameException e) {
                e.printStackTrace();
            }
        }

        public ScyllaJmxHistogram(JsonObject obj, String metricName)
        {
            if (obj.containsKey("hist")) {
                obj = obj.getJsonObject("hist");
            }
            if (obj.containsKey("buckets")) {
                histogram = new Histogram(new EstimatedHistogram(obj));
            } else {
                histogram = new Histogram(obj);
            }

            try {
                objectName=new ObjectName("");
            } catch (MalformedObjectNameException e) {
                e.printStackTrace();
            }
        }

        private Histogram histogram;

        public void update(JsonObject obj) {

        }

        @Override
        public long getCount() {
            return histogram.getCount();
        }

        @Override
        public long getMin() {
            return histogram.getMin();
        }

        @Override
        public long getMax() {
            return histogram.getMax();
        }

        @Override
        public double getMean() {
            return histogram.getMean();
        }

        @Override
        public double getStdDev() {
            return histogram.getStdDev();
        }

        @Override
        public double get50thPercentile() {
            return histogram.getValue(.5);
        }

        @Override
        public double get75thPercentile() {
            return histogram.getValue(.75);
        }

        @Override
        public double get95thPercentile() {
            return histogram.getValue(.95);
        }

        @Override
        public double get98thPercentile() {
            return histogram.getValue(.98);
        }

        @Override
        public double get99thPercentile() {
            return histogram.getValue(.99);
        }

        @Override
        public double get999thPercentile() {
            return histogram.getValue(.999);
        }

        @Override
        public long[] values() {
            return histogram.getValues();
        }

        public static class Histogram {
            private final long count;
            private final long min;
            private final long max;
            private final double mean;
            private final double stdDev;

            private final Samples samples;

            public Histogram(long count, long min, long max, double mean, double stdDev, Samples samples) {
                this.count = count;
                this.min = min;
                this.max = max;
                this.mean = mean;
                this.stdDev = stdDev;
                this.samples = samples;
            }

            public Histogram() {
                this(0, 0, 0, 0, 0, new Samples() {
                });
            }

            public Histogram(JsonObject obj) {
                this(obj.getJsonNumber("count").longValue(), obj.getJsonNumber("min").longValue(),
                        obj.getJsonNumber("max").longValue(), obj.getJsonNumber("mean").doubleValue(),
                        obj.getJsonNumber("variance").doubleValue(), new BufferSamples(getValues(obj)));
            }

            public Histogram(EstimatedHistogram h) {
                this(h.count(), h.min(), h.max(), h.mean(), 0, h);
            }

            private static long[] getValues(JsonObject obj) {
                JsonArray arr = obj.getJsonArray("sample");
                if (arr != null) {
                    return asLongArray(arr);
                }
                return new long[0];
            }

            public long[] getValues() {
                return samples.getValues();
            }

            // Origin (and previous iterations of scylla-jxm)
            // uses biased/ExponentiallyDecaying measurements
            // for the history & quantile resolution.
            // However, for use that is just gobbletigook, since
            // we, at occasions of being asked, and when certain time
            // has passed, ask the actual scylla server for a
            // "values" buffer. A buffer with no information whatsoever
            // on how said values correlate to actual sampling
            // time.
            // So, applying time weights at this level is just
            // wrong. We can just as well treat this as a uniform
            // distribution.
            // Obvious improvement: Send time/value tuples instead.
            public double getValue(double quantile) {
                return samples.getValue(quantile);
            }

            public long getCount() {
                return count;
            }

            public long getMin() {
                return min;
            }

            public long getMax() {
                return max;
            }

            public double getMean() {
                return mean;
            }

            public double getStdDev() {
                return stdDev;
            }
        }

        private static long[] asLongArray(JsonArray a) {
            return a.getValuesAs(JsonNumber.class).stream().mapToLong(n -> n.longValue()).toArray();
        }

        private static interface Samples {
            default double getValue(double quantile) {
                return 0;
            }

            default long[] getValues() {
                return new long[0];
            }
        }

        private static class BufferSamples implements Samples {
            private final long[] samples;

            public BufferSamples(long[] samples) {
                this.samples = samples;
                Arrays.sort(this.samples);
            }

            @Override
            public long[] getValues() {
                return samples;
            }

            @Override
            public double getValue(double quantile) {
                if (quantile < 0.0 || quantile > 1.0) {
                    throw new IllegalArgumentException(quantile + " is not in [0..1]");
                }

                if (samples.length == 0) {
                    return 0.0;
                }

                final double pos = quantile * (samples.length + 1);

                if (pos < 1) {
                    return samples[0];
                }

                if (pos >= samples.length) {
                    return samples[samples.length - 1];
                }

                final double lower = samples[(int) pos - 1];
                final double upper = samples[(int) pos];
                return lower + (pos - floor(pos)) * (upper - lower);
            }
        }

        public static class EstimatedHistogram implements Samples {
            /**
             * The series of values to which the counts in `buckets` correspond: 1,
             * 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 17, 20, etc. Thus, a `buckets` of
             * [0, 0, 1, 10] would mean we had seen one value of 3 and 10 values of
             * 4.
             *
             * The series starts at 1 and grows by 1.2 each time (rounding and
             * removing duplicates). It goes from 1 to around 36M by default
             * (creating 90+1 buckets), which will give us timing resolution from
             * microseconds to 36 seconds, with less precision as the numbers get
             * larger.
             *
             * Each bucket represents values from (previous bucket offset, current
             * offset].
             */
            private final long[] bucketOffsets;
            // buckets is one element longer than bucketOffsets -- the last element
            // is
            // values greater than the last offset
            private long[] buckets;

            public EstimatedHistogram(JsonObject obj) {
                this(asLongArray(obj.getJsonArray("bucket_offsets")), asLongArray(obj.getJsonArray("buckets")));
            }

            public EstimatedHistogram(long[] offsets, long[] bucketData) {
                assert bucketData.length == offsets.length + 1;
                bucketOffsets = offsets;
                buckets = bucketData;
            }

            /**
             * @return the smallest value that could have been added to this
             *         histogram
             */
            public long min() {
                for (int i = 0; i < buckets.length; i++) {
                    if (buckets[i] > 0) {
                        return i == 0 ? 0 : 1 + bucketOffsets[i - 1];
                    }
                }
                return 0;
            }

            /**
             * @return the largest value that could have been added to this
             *         histogram. If the histogram overflowed, returns
             *         Long.MAX_VALUE.
             */
            public long max() {
                int lastBucket = buckets.length - 1;
                if (buckets[lastBucket] > 0) {
                    return Long.MAX_VALUE;
                }

                for (int i = lastBucket - 1; i >= 0; i--) {
                    if (buckets[i] > 0) {
                        return bucketOffsets[i];
                    }
                }
                return 0;
            }

            @Override
            public long[] getValues() {
                return buckets;
            }

            /**
             * @param percentile
             * @return estimated value at given percentile
             */
            @Override
            public double getValue(double percentile) {
                assert percentile >= 0 && percentile <= 1.0;
                int lastBucket = buckets.length - 1;
                if (buckets[lastBucket] > 0) {
                    throw new IllegalStateException("Unable to compute when histogram overflowed");
                }

                long pcount = (long) Math.floor(count() * percentile);
                if (pcount == 0) {
                    return 0;
                }

                long elements = 0;
                for (int i = 0; i < lastBucket; i++) {
                    elements += buckets[i];
                    if (elements >= pcount) {
                        return bucketOffsets[i];
                    }
                }
                return 0;
            }

            /**
             * @return the mean histogram value (average of bucket offsets, weighted
             *         by count)
             * @throws IllegalStateException
             *             if any values were greater than the largest bucket
             *             threshold
             */
            public long mean() {
                int lastBucket = buckets.length - 1;
                if (buckets[lastBucket] > 0) {
                    throw new IllegalStateException("Unable to compute ceiling for max when histogram overflowed");
                }

                long elements = 0;
                long sum = 0;
                for (int i = 0; i < lastBucket; i++) {
                    long bCount = buckets[i];
                    elements += bCount;
                    sum += bCount * bucketOffsets[i];
                }

                return (long) Math.ceil((double) sum / elements);
            }

            /**
             * @return the total number of non-zero values
             */
            public long count() {
                return Arrays.stream(buckets).sum();
            }

            /**
             * @return true if this histogram has overflowed -- that is, a value
             *         larger than our largest bucket could bound was added
             */
            @SuppressWarnings("unused")
            public boolean isOverflowed() {
                return buckets[buckets.length - 1] > 0;
            }

        }

    }
