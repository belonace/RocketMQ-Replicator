package com.belo.rocketmq.replicator.controller.reporter;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by liqiang on 2017/9/27.
 */
public class RomaControllerReporter extends ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RomaControllerReporter.class);

    private final Clock clock;
    private final String prefix;

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public RomaControllerReporter build() {
            return new RomaControllerReporter(registry,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter);
        }
    }

    private RomaControllerReporter(MetricRegistry registry,
                                   Clock clock,
                                   String prefix,
                                   TimeUnit rateUnit,
                                   TimeUnit durationUnit,
                                   MetricFilter filter) {
        super(registry, "roma-reporter", filter, rateUnit, durationUnit);
        this.clock = clock;
        this.prefix = prefix;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        final long timestamp = clock.getTime() / 1000;

        try {

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                reportGauge(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                reportCounter(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                reportHistogram(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                reportMetered(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                reportTimer(entry.getKey(), entry.getValue(), timestamp);
            }

        } catch (IOException e) {
            LOGGER.error("Unable to report to roma", e);
        }
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } finally {
            //
        }
    }

    private void reportTimer(String name, Timer timer, long timestamp) throws IOException {
        final Snapshot snapshot = timer.getSnapshot();

//        KVItem kvItem = new KVItem();
//        kvItem.put(MonitorConstants.KV_ITEM_KEY, name);
//
//        kvItem.put(prefix(name, "max"), format(convertDuration(snapshot.getMax())));
//        kvItem.put(prefix(name, "mean"), format(convertDuration(snapshot.getMean())));
//        kvItem.put(prefix(name, "min"), format(convertDuration(snapshot.getMin())));
//        kvItem.put(prefix(name, "stddev"), format(convertDuration(snapshot.getStdDev())));
//        kvItem.put(prefix(name, "p50"), format(convertDuration(snapshot.getMedian())));
//        kvItem.put(prefix(name, "p75"), format(convertDuration(snapshot.get75thPercentile())));
//        kvItem.put(prefix(name, "p95"), format(convertDuration(snapshot.get95thPercentile())));
//        kvItem.put(prefix(name, "p98"), format(convertDuration(snapshot.get98thPercentile())));
//        kvItem.put(prefix(name, "p99"), format(convertDuration(snapshot.get99thPercentile())));
//        kvItem.put(prefix(name, "p999"), format(convertDuration(snapshot.get999thPercentile())));
//
//        kvItem.submit();
        reportMetered(name, timer, timestamp);
    }

    private void reportMetered(String name, Metered meter, long timestamp) throws IOException {
//        KVItem kvItem = new KVItem();
//        kvItem.put(MonitorConstants.KV_ITEM_KEY, name);
//
//        kvItem.put(prefix(name, "count"), format(meter.getCount()));
//        kvItem.put(prefix(name, "m1_rate"), format(convertRate(meter.getOneMinuteRate())));
//        kvItem.put(prefix(name, "m5_rate"), format(convertRate(meter.getFiveMinuteRate())));
//        kvItem.put(prefix(name, "m15_rate"), format(convertRate(meter.getFifteenMinuteRate())));
//        kvItem.put(prefix(name, "mean_rate"), format(convertRate(meter.getMeanRate())));
//
//        kvItem.submit();
    }

    private void reportHistogram(String name, Histogram histogram, long timestamp) throws IOException {
        final Snapshot snapshot = histogram.getSnapshot();

//        KVItem kvItem = new KVItem();
//        kvItem.put(MonitorConstants.KV_ITEM_KEY, name);
//
//        kvItem.put(prefix(name, "count"), format(histogram.getCount()));
//        kvItem.put(prefix(name, "max"), format(snapshot.getMax()));
//        kvItem.put(prefix(name, "mean"), format(snapshot.getMean()));
//        kvItem.put(prefix(name, "min"), format(snapshot.getMin()));
//        kvItem.put(prefix(name, "stddev"), format(snapshot.getStdDev()));
//        kvItem.put(prefix(name, "p50"), format(snapshot.getMedian()));
//        kvItem.put(prefix(name, "p75"), format(snapshot.get75thPercentile()));
//        kvItem.put(prefix(name, "p95"), format(snapshot.get95thPercentile()));
//        kvItem.put(prefix(name, "p98"), format(snapshot.get98thPercentile()));
//        kvItem.put(prefix(name, "p99"), format(snapshot.get99thPercentile()));
//        kvItem.put(prefix(name, "p999"), format(snapshot.get999thPercentile()));
//
//        kvItem.submit();
    }

    private void reportCounter(String name, Counter counter, long timestamp) throws IOException {
//        KVItem kvItem = new KVItem();
//        kvItem.put(MonitorConstants.KV_ITEM_KEY, name);
//
//        kvItem.put(prefix(name, "count"), format(counter.getCount()));
//
//        kvItem.submit();
    }

    private void reportGauge(String name, Gauge gauge, long timestamp) throws IOException {
        final String value = format(gauge.getValue());
        if (value != null) {

//            KVItem kvItem = new KVItem();
//            kvItem.put(MonitorConstants.KV_ITEM_KEY, name);
//
//            kvItem.put(prefix(name), value);
//
//            kvItem.submit();
        }
    }

    private String format(Object o) {
        if (o instanceof Float) {
            return format(((Float) o).doubleValue());
        } else if (o instanceof Double) {
            return format(((Double) o).doubleValue());
        } else if (o instanceof Byte) {
            return format(((Byte) o).longValue());
        } else if (o instanceof Short) {
            return format(((Short) o).longValue());
        } else if (o instanceof Integer) {
            return format(((Integer) o).longValue());
        } else if (o instanceof Long) {
            return format(((Long) o).longValue());
        }
        return null;
    }

    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

    private String format(long n) {
        return Long.toString(n);
    }

    private String format(double v) {
        return String.format(Locale.CHINA, "%2.2f", v);
    }
}
