package cz.vut.fit.domainradar.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.Common;
import cz.vut.fit.domainradar.KafkaMergedResult;
import cz.vut.fit.domainradar.KafkaDomainAggregate;
import cz.vut.fit.domainradar.KafkaDomainEntry;
import cz.vut.fit.domainradar.KafkaIPEntry;
import cz.vut.fit.domainradar.serialization.TagRegistry;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A Flink sink that stores {@link KafkaMergedResult} objects into a PostgreSQL database.
 * It performs the same operations as the former <code>process_collection_results()</code> procedure.
 */
public class PostgresCollectorResultSink extends RichSinkFunction<KafkaMergedResult> {
    public static final String COMPONENT_NAME = "postgres-sink";
    private static final Logger LOG = Common.getComponentLogger(PostgresCollectorResultSink.class);

    private Connection connection;
    private PreparedStatement selectDomain;
    private PreparedStatement insertDomain;
    private PreparedStatement selectIp;
    private PreparedStatement insertIp;
    private PreparedStatement insertResult;
    private PreparedStatement insertDomainError;

    private ObjectMapper mapper;

    private final Map<String, CollectorInfo> collectorCache = new HashMap<>();

    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext ctx = getRuntimeContext();
        var params = ctx.getExecutionConfig().getGlobalJobParameters();
        if (params != null) {
            dbUrl = params.toMap().get("db.url");
            dbUser = params.toMap().get("db.user");
            dbPassword = params.toMap().get("db.password");
        }

        if (dbUrl == null) {
            throw new IllegalStateException("Database URL not specified (db.url)");
        }
        connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        connection.setAutoCommit(false);

        selectDomain = connection.prepareStatement("SELECT id FROM Domain WHERE domain_name = ?");
        insertDomain = connection.prepareStatement(
                "INSERT INTO Domain(domain_name, last_update) VALUES (?, ?) RETURNING id");
        selectIp = connection.prepareStatement(
                "SELECT id FROM IP WHERE domain_id = ? AND ip = ?::inet");
        insertIp = connection.prepareStatement(
                "INSERT INTO IP(domain_id, ip) VALUES (?, ?::inet) ON CONFLICT (domain_id, ip) DO NOTHING RETURNING id");
        insertResult = connection.prepareStatement(
                "INSERT INTO Collection_Result(domain_id, ip_id, source_id, status_code, error, timestamp, raw_data)" +
                        " VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)" +
                        " ON CONFLICT ON CONSTRAINT collection_result_unique DO UPDATE" +
                        " SET status_code=EXCLUDED.status_code, error=EXCLUDED.error, raw_data=EXCLUDED.raw_data");
        insertDomainError = connection.prepareStatement(
                "INSERT INTO Domain_Errors(domain_id, timestamp, source, error, sql_error_code, sql_error_message)" +
                        " VALUES (?, ?, ?, ?, ?, ?)");
        mapper = Common.makeMapper().build();
    }

    @Override
    public void invoke(KafkaMergedResult value, Context context) {
        try {
            processResult(value);
            connection.commit();
        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                LOG.error("Rollback failed", ex);
            }
            LOG.error("Failed to store merged result", e);
        }
    }

    private void processResult(KafkaMergedResult merged) throws Exception {
        var domainName = merged.getDomainName();
        long ts = earliestTimestamp(merged);
        long domainId = getOrCreateDomain(domainName, ts);

        Map<String, Long> ipIds = new HashMap<>();
        if (merged.getIPData() != null) {
            for (var ipEntry : merged.getIPData().entrySet()) {
                var ipId = getOrCreateIp(domainId, ipEntry.getKey());
                ipIds.put(ipEntry.getKey(), ipId);
            }
        }

        // Domain based results
        var domainData = merged.getDomainData();
        handleDomainEntry(domainId, domainData.getZoneData());
        handleDomainEntry(domainId, domainData.getDNSData());
        handleDomainEntry(domainId, domainData.getRDAPData());
        handleDomainEntry(domainId, domainData.getTLSData());

        // IP based results
        if (merged.getIPData() != null) {
            for (var ipEntry : merged.getIPData().entrySet()) {
                var ipId = ipIds.get(ipEntry.getKey());
                for (var collectorEntry : ipEntry.getValue().entrySet()) {
                    handleIpEntry(domainId, ipId, collectorEntry.getKey(), collectorEntry.getValue());
                }
            }
        }
    }

    private long getOrCreateDomain(String domainName, long timestamp) throws SQLException {
        selectDomain.setString(1, domainName);
        try (var rs = selectDomain.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        insertDomain.setString(1, domainName);
        insertDomain.setTimestamp(2, new Timestamp(timestamp));
        try (var rs = insertDomain.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        // Should not happen, but fallback
        selectDomain.setString(1, domainName);
        try (var rs = selectDomain.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private long getOrCreateIp(long domainId, String ip) throws SQLException {
        selectIp.setLong(1, domainId);
        selectIp.setString(2, ip);
        try (var rs = selectIp.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        insertIp.setLong(1, domainId);
        insertIp.setString(2, ip);
        try (var rs = insertIp.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            // Invalid IP literal - fallback to 0.0.0.0
            insertIp.setLong(1, domainId);
            insertIp.setString(2, "0.0.0.0");
            try (var rs2 = insertIp.executeQuery()) {
                if (rs2.next()) {
                    return rs2.getLong(1);
                }
            }
        }
        selectIp.setLong(1, domainId);
        selectIp.setString(2, ip);
        try (var rs = selectIp.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private void handleDomainEntry(long domainId, KafkaDomainEntry entry) throws Exception {
        if (entry == null) {
            return;
        }
        String collector = cz.vut.fit.domainradar.Topics.TOPICS_TO_COLLECTOR_ID.get(entry.getTopic());
        if (collector == null) {
            insertDomainError(domainId, entry.getTimestamp(), "pg-sink", "Unknown collector: " + entry.getTopic(), null);
            return;
        }
        CollectorInfo ci = getCollectorInfo(domainId, entry.getTimestamp(), collector);
        if (ci == null) {
            return; // error already logged
        }
        JsonNode node = parseJson(domainId, entry.getTimestamp(), entry.getValue());
        String error = node != null && node.hasNonNull("error") ? node.get("error").asText() : null;
        insertCollectionResult(domainId, null, ci.id, entry.getStatusCode(), error, entry.getTimestamp(), entry.getValue());
    }

    private void handleIpEntry(long domainId, long ipId, byte collectorTag, KafkaIPEntry entry) throws Exception {
        String collector = TagRegistry.COLLECTOR_NAMES.get((int) collectorTag);
        if (collector == null) {
            insertDomainError(domainId, entry.getTimestamp(), "pg-sink", "Unknown collector tag: " + collectorTag, null);
            return;
        }
        CollectorInfo ci = getCollectorInfo(domainId, entry.getTimestamp(), collector);
        if (ci == null) return;
        JsonNode node = parseJson(domainId, entry.getTimestamp(), entry.getValue());
        String error = node != null && node.hasNonNull("error") ? node.get("error").asText() : null;
        insertCollectionResult(domainId, ipId, ci.id, entry.getStatusCode(), error, entry.getTimestamp(), entry.getValue());
        if (node != null) {
            updateIpData(ipId, collector, node, entry.getTimestamp(), entry.getStatusCode());
        }
    }

    private JsonNode parseJson(long domainId, long ts, byte[] data) throws SQLException {
        try {
            return mapper.readTree(data);
        } catch (Exception e) {
            insertDomainError(domainId, ts, "pg-sink", "Cannot parse JSON.", e);
            return null;
        }
    }

    private void insertDomainError(long domainId, long ts, String source, String message, Exception e) throws SQLException {
        insertDomainError.setLong(1, domainId);
        insertDomainError.setTimestamp(2, new Timestamp(ts));
        insertDomainError.setString(3, source);
        insertDomainError.setString(4, message);
        if (e instanceof SQLException se) {
            insertDomainError.setString(5, se.getSQLState());
            insertDomainError.setString(6, se.getMessage());
        } else {
            insertDomainError.setNull(5, Types.VARCHAR);
            insertDomainError.setString(6, e == null ? null : e.getMessage());
        }
        insertDomainError.executeUpdate();
    }

    private CollectorInfo getCollectorInfo(long domainId, long ts, String name) throws SQLException {
        CollectorInfo ci = collectorCache.get(name);
        if (ci != null) return ci;
        try (var ps = connection.prepareStatement("SELECT id, is_ip_collector FROM Collector WHERE collector = ?")) {
            ps.setString(1, name);
            try (var rs = ps.executeQuery()) {
                if (rs.next()) {
                    ci = new CollectorInfo(rs.getShort(1), rs.getBoolean(2));
                    collectorCache.put(name, ci);
                    return ci;
                }
            }
        }
        insertDomainError(domainId, ts, "pg-sink", "Unknown collector: " + name, null);
        return null;
    }

    private void insertCollectionResult(long domainId, Long ipId, short collectorId, int statusCode,
                                        String error, long ts, byte[] rawData) throws SQLException {
        insertResult.setLong(1, domainId);
        if (ipId == null) {
            insertResult.setNull(2, Types.BIGINT);
        } else {
            insertResult.setLong(2, ipId);
        }
        insertResult.setShort(3, collectorId);
        insertResult.setInt(4, statusCode);
        if (error == null) {
            insertResult.setNull(5, Types.VARCHAR);
        } else {
            insertResult.setString(5, error);
        }
        insertResult.setTimestamp(6, new Timestamp(ts));
        insertResult.setString(7, new String(rawData));
        insertResult.executeUpdate();
    }

    private void updateIpData(long ipId, String collector, JsonNode data, long ts, int status) throws SQLException {
        if (ipId == 0 || data == null || status != 0) return;
        if ("geo-asn".equals(collector)) {
            JsonNode d = data.get("data");
            if (d == null || !d.isObject()) return;
            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE IP SET geo_country_code=?, geo_region=?, geo_region_code=?, geo_city=?," +
                            " geo_postal_code=?, geo_latitude=?, geo_longitude=?, geo_timezone=?," +
                            " asn=?, as_org=?, network_address=?, network_prefix_length=?," +
                            " geo_asn_update_timestamp=? WHERE id=?" +
                            " AND (geo_asn_update_timestamp IS NULL OR geo_asn_update_timestamp <= ?)")) {
                ps.setString(1, textOrNull(d, "countryCode"));
                ps.setString(2, textOrNull(d, "region"));
                ps.setString(3, textOrNull(d, "regionCode"));
                ps.setString(4, textOrNull(d, "city"));
                ps.setString(5, textOrNull(d, "postalCode"));
                setDoubleOrNull(ps, 6, d.get("latitude"));
                setDoubleOrNull(ps, 7, d.get("longitude"));
                ps.setString(8, textOrNull(d, "timezone"));
                setLongOrNull(ps, 9, d.get("asn"));
                ps.setString(10, textOrNull(d, "asnOrg"));
                ps.setString(11, textOrNull(d, "networkAddress"));
                setIntOrNull(ps, 12, d.get("prefixLength"));
                Timestamp tsObj = new Timestamp(ts);
                ps.setTimestamp(13, tsObj);
                ps.setLong(14, ipId);
                ps.setTimestamp(15, tsObj);
                ps.executeUpdate();
            }
        } else if ("nerd".equals(collector)) {
            JsonNode rep = data.path("data").get("reputation");
            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE IP SET nerd_reputation=?, nerd_update_timestamp=? WHERE id=?" +
                            " AND (nerd_update_timestamp IS NULL OR nerd_update_timestamp <= ?)")) {
                setDoubleOrNull(ps, 1, rep);
                Timestamp tsObj = new Timestamp(ts);
                ps.setTimestamp(2, tsObj);
                ps.setLong(3, ipId);
                ps.setTimestamp(4, tsObj);
                ps.executeUpdate();
            }
        }
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode n = node.get(field);
        return n == null || n.isNull() ? null : n.asText();
    }

    private static void setDoubleOrNull(PreparedStatement ps, int idx, JsonNode n) throws SQLException {
        if (n == null || n.isNull()) {
            ps.setNull(idx, Types.DOUBLE);
        } else {
            ps.setDouble(idx, n.asDouble());
        }
    }

    private static void setLongOrNull(PreparedStatement ps, int idx, JsonNode n) throws SQLException {
        if (n == null || n.isNull()) {
            ps.setNull(idx, Types.BIGINT);
        } else {
            ps.setLong(idx, n.asLong());
        }
    }

    private static void setIntOrNull(PreparedStatement ps, int idx, JsonNode n) throws SQLException {
        if (n == null || n.isNull()) {
            ps.setNull(idx, Types.INTEGER);
        } else {
            ps.setInt(idx, n.asInt());
        }
    }

    private long earliestTimestamp(KafkaMergedResult merged) {
        long ts = Long.MAX_VALUE;
        KafkaDomainAggregate d = merged.getDomainData();
        if (d.getZoneData() != null) ts = Math.min(ts, d.getZoneData().getTimestamp());
        if (d.getDNSData() != null) ts = Math.min(ts, d.getDNSData().getTimestamp());
        if (d.getRDAPData() != null) ts = Math.min(ts, d.getRDAPData().getTimestamp());
        if (d.getTLSData() != null) ts = Math.min(ts, d.getTLSData().getTimestamp());
        if (merged.getIPData() != null) {
            for (var ip : merged.getIPData().values()) {
                for (KafkaIPEntry e : ip.values()) {
                    ts = Math.min(ts, e.getTimestamp());
                }
            }
        }
        if (ts == Long.MAX_VALUE) ts = System.currentTimeMillis();
        return ts;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    private record CollectorInfo(short id, boolean isIpCollector) { }
}

