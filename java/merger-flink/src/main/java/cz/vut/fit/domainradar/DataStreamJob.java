package cz.vut.fit.domainradar;

import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;


public class DataStreamJob {

    private static final Properties kafkaProperties = new Properties();
    private static final Properties appProperties = new Properties();
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

    public static void main(String[] args) throws Exception {
        // ==== Configuration ====
        // All configuration properties with the "kafka." prefix will be passed to the Kafka source
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);
        LOG.info("Setting application properties:");
        params.toMap().forEach((k, v) -> {
            if (k.startsWith("kafka.")) {
                kafkaProperties.setProperty(k.substring(6), v);
            } else {
                LOG.info("\t{}: {}", k, v);
                appProperties.setProperty(k, v);
            }
        });

        if (params.getBoolean(MergerConfig.IP_DISABLE_NERD, false)) {
            TagRegistry.TAGS.remove("nerd");
        }

        // ==== Flink execution environment ====
        final Configuration pipelineConfig = new Configuration();
        //pipelineConfig.set(PipelineOptions.FORCE_AVRO, Boolean.TRUE);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(pipelineConfig);
        env.getConfig().setGlobalJobParameters(params);

        // ==== Checkpointing ====
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        final var checkpointConfig = env.getCheckpointConfig();
        // Retain the last checkpoint both when the job fails and when it is manually cancelled
        checkpointConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // Checkpoints may sometimes take longer than on average, don't congest the system
        // with additional checkpoint runs
        checkpointConfig.setMinPauseBetweenCheckpoints(5000);
        // Checkpoints have to complete within 30 seconds, or are discarded
        checkpointConfig.setCheckpointTimeout(30000);
        // Two consecutive checkpoint failures are tolerated
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        // Allow only one checkpoint to be in progress at the same time
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // TODO: Determine if unaligned checkpoints help the pipeline
        //       (especially under heavy load)
        // checkpointConfig.enableUnalignedCheckpoints();

        // Create the pipeline
        makePipeline(env);
        //makeTestPipeline(env);

        // ==== Execution ====
        env.execute("DomainRadar Data Merger");
    }

    private static void makeTestPipeline(StreamExecutionEnvironment env) {
        var zoneStream = makeKafkaDomainStream(env, Topics.OUT_ZONE);
        var dnsStream = makeKafkaDomainStream(env, Topics.OUT_DNS);
        var tlsStream = makeKafkaDomainStream(env, Topics.OUT_TLS);
        var rdapDnStream = makeKafkaDomainStream(env, Topics.OUT_RDAP_DN);
        //var ipDataStream = makeKafkaIpStream(env, Topics.OUT_IP)
        //        .keyBy(KafkaIPEntry::getDomainName);

        zoneStream.union(dnsStream, tlsStream, rdapDnStream)
                .keyBy(KafkaDomainEntry::getDomainName)
                .process(new DomainEntriesProcessFunction())
                .uid("dn-merging-processor")
                .print();
    }

    private static void makePipeline(StreamExecutionEnvironment env) {
        // ==== Sources & Sinks ====
        KafkaSink<Tuple2<String, byte[]>> sink = KafkaSink.<Tuple2<String, byte[]>>builder()
                .setKafkaProducerConfig(kafkaProperties)
                .setRecordSerializer(new KafkaSerializer<>(Topics.OUT_MERGE_ALL, true))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        var zoneStream = makeKafkaDomainStream(env, Topics.OUT_ZONE);
        var dnsStream = makeKafkaDomainStream(env, Topics.OUT_DNS);
        var tlsStream = makeKafkaDomainStream(env, Topics.OUT_TLS);
        var rdapDnStream = makeKafkaDomainStream(env, Topics.OUT_RDAP_DN);
        var ipDataStream = makeKafkaIpStream(env, Topics.OUT_IP)
                .keyBy(KafkaIPEntry::getDomainName);

        // ==== The pipeline ====
        var dnData = zoneStream.union(dnsStream, tlsStream, rdapDnStream)
                .keyBy(KafkaDomainEntry::getDomainName)
                .process(new DomainEntriesProcessFunction())
                .uid("dn-merging-processor")
                .keyBy(KafkaDomainAggregate::getDomainName);

        // Only use the IP merger for entries that actually carried IPs to process
        var dnDataWithIps = dnData
                .filter(dnAggregate -> !dnAggregate.getDNSIPs().isEmpty())
                .uid("dn-data-with-ips-filter")
                .keyBy(KafkaDomainAggregate::getDomainName);
        var mergedResults = ipDataStream.connect(dnDataWithIps)
                .process(new IPEntriesProcessFunction())
                .uid("dn-ip-final-merging-processor");
        var pgSink = new cz.vut.fit.domainradar.db.PostgresCollectorResultSink(
                appProperties.getProperty("db.url"),
                appProperties.getProperty("db.user"),
                appProperties.getProperty("db.password"));
        mergedResults
                .sinkTo(pgSink)
                .uid("postgres-sink-with-ips");
        var mergedData = mergedResults
                .map(new SerdeMappingFunction())
                .uid("serde-mapper-with-ips")
                .filter(tuple -> tuple.f0 != null)
                .uid("result-with-ips-not-null-filter");
        mergedData
                .sinkTo(sink)
                .uid("entries-with-ips-sink");

        // Handle the IP-less merged results from DN-based collectors
        var dnDataWithoutIps = dnData
                .filter(dnAggregate -> dnAggregate.getDNSIPs().isEmpty())
                .uid("dn-data-without-ips-filter")
                .keyBy(KafkaDomainAggregate::getDomainName);
        var resultsWithoutIpsKM = dnDataWithoutIps
                .map(dnAggregate -> new KafkaMergedResult(dnAggregate.getDomainName(), dnAggregate, null))
                .uid("map-to-merged-results");
        resultsWithoutIpsKM
                .sinkTo(pgSink)
                .uid("postgres-sink-without-ips");
        var resultsWithoutIps = resultsWithoutIpsKM
                .map(new SerdeMappingFunction())
                .uid("serde-mapper-without-ips")
                .filter(tuple -> tuple.f0 != null)
                .uid("result-without-ips-not-null-filter");
        resultsWithoutIps
                .sinkTo(sink)
                .uid("entries-without-ips-sink");
    }

    private static KeyedStream<KafkaDomainEntry, String> makeKafkaDomainStream(final StreamExecutionEnvironment env,
                                                                               final String topic) {
        final var outOfOrdernessMs =
                Long.parseLong(appProperties.getProperty(MergerConfig.DN_MAX_OUT_OF_ORDERNESS_MS_CONFIG,
                        MergerConfig.DN_MAX_OUT_OF_ORDERNESS_MS_DEFAULT));
        final var idlenessSec =
                Long.parseLong(appProperties.getProperty(MergerConfig.DN_IDLENESS_SEC_CONFIG,
                        MergerConfig.DN_IDLENESS_SEC_DEFAULT));

        return env.fromSource(makeKafkaDomainSource(topic), makeWatermarkStrategy(outOfOrdernessMs, idlenessSec), "Kafka: " + topic)
                .uid("source-kafka-dn-" + topic)
                .keyBy(KafkaDomainEntry::getDomainName);
    }

    private static KeyedStream<KafkaIPEntry, String>
    makeKafkaIpStream(final StreamExecutionEnvironment env, final String topic) {
        final var outOfOrdernessMs =
                Long.parseLong(appProperties.getProperty(MergerConfig.IP_MAX_OUT_OF_ORDERNESS_MS_CONFIG,
                        MergerConfig.IP_MAX_OUT_OF_ORDERNESS_MS_DEFAULT));
        final var idlenessSec =
                Long.parseLong(appProperties.getProperty(MergerConfig.IP_IDLENESS_SEC_CONFIG,
                        MergerConfig.IP_IDLENESS_SEC_DEFAULT));

        return env.fromSource(makeKafkaIpSource(topic), makeWatermarkStrategy(outOfOrdernessMs, idlenessSec), "Kafka: " + topic)
                .uid("source-kafka-ip-" + topic)
                .keyBy(KafkaIPEntry::getDomainName);
    }

    private static KafkaSource<KafkaDomainEntry> makeKafkaDomainSource(final String topic) {
        return KafkaSource.<KafkaDomainEntry>builder()
                .setProperties(kafkaProperties)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaDomainEntryDeserializer())
                .build();
    }

    private static KafkaSource<KafkaIPEntry> makeKafkaIpSource(final String topic) {
        return KafkaSource.<KafkaIPEntry>builder()
                .setProperties(kafkaProperties)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaIPEntryDeserializer())
                .build();
    }

    private static <T> WatermarkStrategy<T> makeWatermarkStrategy(long outOfOrdernessMs, long idlenessSec) {
        var strategy = WatermarkStrategy
                .<T>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrdernessMs));

        if (idlenessSec > 0) {
            return strategy.withIdleness(Duration.ofSeconds(idlenessSec));
        } else {
            return strategy;
        }
    }
}
