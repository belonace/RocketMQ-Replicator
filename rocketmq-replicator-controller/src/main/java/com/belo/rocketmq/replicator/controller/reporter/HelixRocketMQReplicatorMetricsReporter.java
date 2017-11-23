package com.belo.rocketmq.replicator.controller.reporter;

import com.belo.rocketmq.replicator.controller.ControllerConf;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * 指标汇报
 * Created by liqiang on 2017/9/27.
 */
public class HelixRocketMQReplicatorMetricsReporter {

    private static final String ROCKETMQ_METRICS_REPORTER_PREFIX_FORMAT =
            "stats.%s.counter.rocketmq-replicator-controller.%s.%s";

    private static HelixRocketMQReplicatorMetricsReporter METRICS_REPORTER_INSTANCE = null;

    private static final Logger LOGGER = Logger.getLogger(HelixRocketMQReplicatorMetricsReporter.class);

    private static volatile boolean DID_INIT = false;

    private final MetricRegistry registry;

    //此处使用roma上报业务指标
    private final RomaControllerReporter romaControllerReporter;

    private final String reporterMetricPrefix;

    HelixRocketMQReplicatorMetricsReporter(ControllerConf config) {
        final String clientId = config.getInstanceId();
        this.reporterMetricPrefix = String.format(ROCKETMQ_METRICS_REPORTER_PREFIX_FORMAT, "", "", clientId);
        LOGGER.info("Reporter Metric Prefix is : " + this.reporterMetricPrefix);
        this.registry = new MetricRegistry();
        final long graphiteReportFreqSec = 10L;

        // Init graphite reporter

        romaControllerReporter =
                RomaControllerReporter.forRegistry(this.registry).prefixedWith(this.reporterMetricPrefix)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL).build();

        romaControllerReporter.start(graphiteReportFreqSec, TimeUnit.SECONDS);


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    Closeables.close(romaControllerReporter, true);
                } catch (Exception e) {
                    LOGGER.error("Error while closing roma reporters.", e);
                }
            }
        });
    }

    private String[] parse(String environment) {
        if (environment == null || environment.trim().length() <= 0) {
            return null;
        }
        String[] res = environment.split("\\.");
        if (res == null || res.length != 2) {
            return null;
        }
        return res;
    }

    /**
     * This function must be called before calling the get() method, because of
     * the dependency on the config object.
     *
     * @param config Specifies config pertaining to Metrics
     */
    public static synchronized void init(ControllerConf config) {
        if (DID_INIT) {
            return;
        }
        METRICS_REPORTER_INSTANCE = new HelixRocketMQReplicatorMetricsReporter(config);
        DID_INIT = true;
    }

    public static HelixRocketMQReplicatorMetricsReporter get() {
        Preconditions.checkState(DID_INIT, "Not initialized yet");
        return METRICS_REPORTER_INSTANCE;
    }

    public MetricRegistry getRegistry() {
        Preconditions.checkState(DID_INIT, "Not initialized yet");
        return this.registry;
    }

    public <T extends com.codahale.metrics.Metric> void registerMetric(String metricName, T metric) {
        Preconditions.checkState(DID_INIT, "Not initialized yet");
        if (this.registry != null) {
            this.registry.register(metricName, metric);
        }
    }

}
