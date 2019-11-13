package org.apache.cassandra.metrics;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.metrics.ScyllaJmxHistogram.Histogram;

public class ScyllaJmxTimer  implements CassandraMetricsRegistry.JmxTimerMBean,CassandraMetricsRegistry.MetricMBean,CassandraMetricsRegistry.JmxMeterMBean
{
    private ObjectName objectName;

    @Override
    public ObjectName objectName()
    {
        return objectName;
    }

    private static final TimeUnit RATE_UNIT = TimeUnit.SECONDS;
    private static final TimeUnit DURATION_UNIT = TimeUnit.MICROSECONDS;
    private static final TimeUnit API_DURATION_UNIT = TimeUnit.MICROSECONDS;
    private static final double DURATION_FACTOR = 1.0 / API_DURATION_UNIT.convert(1, DURATION_UNIT);

    public ScyllaJmxTimer() {
        meter = new Meter();
        histogram = new Histogram();
        try {
            objectName=new ObjectName("");
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        }
    }

    private Histogram histogram;

    public ScyllaJmxTimer(JsonObject obj, String metricName) {
        // TODO: this is not atomic.
        meter = new Meter(obj.getJsonObject("meter"));
        histogram = new Histogram(obj.getJsonObject("hist"));
        try {
            objectName=new ObjectName("");
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        }
    }

    @Override
    public double getMin() {
        return toDuration(histogram.getMin());
    }

    @Override
    public double getMax() {
        return toDuration(histogram.getMax());
    }

    @Override
    public double getMean() {
        return toDuration(histogram.getMean());
    }

    @Override
    public double getStdDev() {
        return toDuration(histogram.getStdDev());
    }

    @Override
    public double get50thPercentile() {
        return toDuration(histogram.getValue(.5));
    }

    @Override
    public double get75thPercentile() {
        return toDuration(histogram.getValue(.75));
    }

    @Override
    public double get95thPercentile() {
        return toDuration(histogram.getValue(.95));
    }

    @Override
    public double get98thPercentile() {
        return toDuration(histogram.getValue(.98));
    }

    @Override
    public double get99thPercentile() {
        return toDuration(histogram.getValue(.99));
    }

    @Override
    public double get999thPercentile() {
        return toDuration(histogram.getValue(.999));
    }

    @Override
    public long[] values() {
        return histogram.getValues();
    }

    @Override
    public String getDurationUnit() {
        return DURATION_UNIT.toString().toLowerCase(Locale.US);
    }

    @Override
    public long getCount() {
        return meter.count;
    }

    @Override
    public double getMeanRate() {
        return meter.meanRate;
    }

    @Override
    public double getOneMinuteRate() {
        return meter.oneMinuteRate;
    }

    @Override
    public double getFiveMinuteRate() {
        return meter.fiveMinuteRate;
    }

    @Override
    public double getFifteenMinuteRate() {
        return meter.fifteenMinuteRate;
    }

    @Override
    public String getRateUnit() {
        return "event/" + unitString(RATE_UNIT);
    }

    private static double toDuration(double micro) {
        return micro * DURATION_FACTOR;
    }

    private Meter meter = new Meter();

    private static class Meter {
        public final long count;
        public final double oneMinuteRate;
        public final double fiveMinuteRate;
        public final double fifteenMinuteRate;
        public final double meanRate;

        public Meter(long count, double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate,
                     double meanRate) {
            this.count = count;
            this.oneMinuteRate = oneMinuteRate;
            this.fiveMinuteRate = fiveMinuteRate;
            this.fifteenMinuteRate = fifteenMinuteRate;
            this.meanRate = meanRate;
        }

        public Meter() {
            this(0, 0, 0, 0, 0);
        }

        public Meter(JsonObject obj) {
            JsonArray rates = obj.getJsonArray("rates");
            oneMinuteRate = rates.getJsonNumber(0).doubleValue();
            fiveMinuteRate = rates.getJsonNumber(1).doubleValue();
            fifteenMinuteRate = rates.getJsonNumber(2).doubleValue();
            meanRate = obj.getJsonNumber("mean_rate").doubleValue();
            count = obj.getJsonNumber("count").longValue();
        }
    }

    private static String unitString(TimeUnit u) {
        String s = u.toString().toLowerCase(Locale.US);
        return s.substring(0, s.length() - 1);
    }

}
