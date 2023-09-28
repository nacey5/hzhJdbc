package com.hzh.server;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.metrics.Gauge;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.TypedValue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.calcite.avatica.remote.MetricsHelper.concat;

/**
 * @ClassName HzhServerMeta
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 14:13
 * @Version 0.0.1
 **/
public class HzhServerMeta implements ProtobufMeta {

    private final String url;
    private final Properties info;
    private final Cache<String, Connection> connectionCache;
//    private final Cache<Integer, StatementInfo> statementCache;
    private final MetricsSystem metrics;

    private static final String CONN_CACHE_KEY_BASE = "avatica.connectioncache";

    private static final String STMT_CACHE_KEY_BASE = "avatica.statementcache";

    public HzhServerMeta(String url) throws SQLException {
        this(url, new Properties());
    }

    public HzhServerMeta(String url, Properties info) throws SQLException {
        this(url, info, NoopMetricsSystem.getInstance());
    }

    public HzhServerMeta(String url, Properties info, MetricsSystem metrics) throws SQLException {
        this.url = url;
        this.info = info;
        this.metrics = Objects.requireNonNull(metrics);

        int concurrencyLevel = Integer.parseInt(
                info.getProperty(ConnectionCacheSettings.CONCURRENCY_LEVEL.key(),
                        ConnectionCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
        int initialCapacity = Integer.parseInt(
                info.getProperty(ConnectionCacheSettings.INITIAL_CAPACITY.key(),
                        ConnectionCacheSettings.INITIAL_CAPACITY.defaultValue()));
        long maxCapacity = Long.parseLong(
                info.getProperty(ConnectionCacheSettings.MAX_CAPACITY.key(),
                        ConnectionCacheSettings.MAX_CAPACITY.defaultValue()));
        long connectionExpiryDuration = Long.parseLong(
                info.getProperty(ConnectionCacheSettings.EXPIRY_DURATION.key(),
                        ConnectionCacheSettings.EXPIRY_DURATION.defaultValue()));
        TimeUnit connectionExpiryUnit = TimeUnit.valueOf(
                info.getProperty(ConnectionCacheSettings.EXPIRY_UNIT.key(),
                        ConnectionCacheSettings.EXPIRY_UNIT.defaultValue()));
        this.connectionCache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(initialCapacity)
                .maximumSize(maxCapacity)
                .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
                .removalListener(new ConnectionExpiryHandler())
                .build();
////        LOG.debug("instantiated connection cache: {}", connectionCache.stats());
//
//        concurrencyLevel = Integer.parseInt(
//                info.getProperty(StatementCacheSettings.CONCURRENCY_LEVEL.key(),
//                        StatementCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
//        initialCapacity = Integer.parseInt(
//                info.getProperty(StatementCacheSettings.INITIAL_CAPACITY.key(),
//                        StatementCacheSettings.INITIAL_CAPACITY.defaultValue()));
//        maxCapacity = Long.parseLong(
//                info.getProperty(StatementCacheSettings.MAX_CAPACITY.key(),
//                        StatementCacheSettings.MAX_CAPACITY.defaultValue()));
//        connectionExpiryDuration = Long.parseLong(
//                info.getProperty(StatementCacheSettings.EXPIRY_DURATION.key(),
//                        StatementCacheSettings.EXPIRY_DURATION.defaultValue()));
//        connectionExpiryUnit = TimeUnit.valueOf(
//                info.getProperty(StatementCacheSettings.EXPIRY_UNIT.key(),
//                        StatementCacheSettings.EXPIRY_UNIT.defaultValue()));
//        this.statementCache = CacheBuilder.newBuilder()
//                .concurrencyLevel(concurrencyLevel)
//                .initialCapacity(initialCapacity)
//                .maximumSize(maxCapacity)
//                .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
//                .removalListener(new StatementExpiryHandler())
//                .build();

//        LOG.debug("instantiated statement cache: {}", statementCache.stats());

        // Register some metrics
        this.metrics.register(concat(
                HzhServerMeta.class, "ConnectionCacheSize"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return connectionCache.size();
            }
        });

        this.metrics.register(concat(HzhServerMeta.class, "StatementCacheSize"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return connectionCache.size();
            }
        });
    }

    @Override
        public ExecuteBatchResult executeBatchProtobuf(StatementHandle statementHandle, List<Requests.UpdateBatch> list) throws NoSuchStatementException {
        return null;
    }

    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle connectionHandle) {
        return null;
    }

    @Override
    public MetaResultSet getTables(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1, List<String> list) {
        return null;
    }

    @Override
    public MetaResultSet getColumns(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1, Pat pat2) {
        return null;
    }

    @Override
    public MetaResultSet getSchemas(ConnectionHandle connectionHandle, String s, Pat pat) {
        return null;
    }

    @Override
    public MetaResultSet getCatalogs(ConnectionHandle connectionHandle) {
        return null;
    }

    @Override
    public MetaResultSet getTableTypes(ConnectionHandle connectionHandle) {
        return null;
    }

    @Override
    public MetaResultSet getProcedures(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1) {
        return null;
    }

    @Override
    public MetaResultSet getProcedureColumns(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1, Pat pat2) {
        return null;
    }

    @Override
    public MetaResultSet getColumnPrivileges(ConnectionHandle connectionHandle, String s, String s1, String s2, Pat pat) {
        return null;
    }

    @Override
    public MetaResultSet getTablePrivileges(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1) {
        return null;
    }

    @Override
    public MetaResultSet getBestRowIdentifier(ConnectionHandle connectionHandle, String s, String s1, String s2, int i, boolean b) {
        return null;
    }

    @Override
    public MetaResultSet getVersionColumns(ConnectionHandle connectionHandle, String s, String s1, String s2) {
        return null;
    }

    @Override
    public MetaResultSet getPrimaryKeys(ConnectionHandle connectionHandle, String s, String s1, String s2) {
        return null;
    }

    @Override
    public MetaResultSet getImportedKeys(ConnectionHandle connectionHandle, String s, String s1, String s2) {
        return null;
    }

    @Override
    public MetaResultSet getExportedKeys(ConnectionHandle connectionHandle, String s, String s1, String s2) {
        return null;
    }

    @Override
    public MetaResultSet getCrossReference(ConnectionHandle connectionHandle, String s, String s1, String s2, String s3, String s4, String s5) {
        return null;
    }

    @Override
    public MetaResultSet getTypeInfo(ConnectionHandle connectionHandle) {
        return null;
    }

    @Override
    public MetaResultSet getIndexInfo(ConnectionHandle connectionHandle, String s, String s1, String s2, boolean b, boolean b1) {
        return null;
    }

    @Override
    public MetaResultSet getUDTs(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1, int[] ints) {
        return null;
    }

    @Override
    public MetaResultSet getSuperTypes(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1) {
        return null;
    }

    @Override
    public MetaResultSet getSuperTables(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1) {
        return null;
    }

    @Override
    public MetaResultSet getAttributes(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1, Pat pat2) {
        return null;
    }

    @Override
    public MetaResultSet getClientInfoProperties(ConnectionHandle connectionHandle) {
        return null;
    }

    @Override
    public MetaResultSet getFunctions(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1) {
        return null;
    }

    @Override
    public MetaResultSet getFunctionColumns(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1, Pat pat2) {
        return null;
    }

    @Override
    public MetaResultSet getPseudoColumns(ConnectionHandle connectionHandle, String s, Pat pat, Pat pat1, Pat pat2) {
        return null;
    }

    @Override
    public Iterable<Object> createIterable(StatementHandle statementHandle, QueryState queryState, Signature signature, List<TypedValue> list, Frame frame) {
        return null;
    }

    @Override
    public StatementHandle prepare(ConnectionHandle connectionHandle, String s, long l) {
        return null;
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle statementHandle, String s, long l, PrepareCallback prepareCallback) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle statementHandle, String s, long l, int i, PrepareCallback prepareCallback) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle statementHandle, List<String> list) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle statementHandle, List<List<TypedValue>> list) throws NoSuchStatementException {
        return null;
    }

    @Override
    public Frame fetch(StatementHandle statementHandle, long l, int i) throws NoSuchStatementException, MissingResultsException {
        return null;
    }

    @Override
    public ExecuteResult execute(StatementHandle statementHandle, List<TypedValue> list, long l) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteResult execute(StatementHandle statementHandle, List<TypedValue> list, int i) throws NoSuchStatementException {
        return null;
    }

    @Override
    public StatementHandle createStatement(ConnectionHandle connectionHandle) {
        return null;
    }

    @Override
    public void closeStatement(StatementHandle statementHandle) {

    }

    @Override
    public void openConnection(ConnectionHandle connectionHandle, Map<String, String> map) {

    }

    @Override
    public void closeConnection(ConnectionHandle connectionHandle) {

    }

    @Override
    public boolean syncResults(StatementHandle statementHandle, QueryState queryState, long l) throws NoSuchStatementException {
        return false;
    }

    @Override
    public void commit(ConnectionHandle connectionHandle) {

    }

    @Override
    public void rollback(ConnectionHandle connectionHandle) {

    }

    @Override
    public ConnectionProperties connectionSync(ConnectionHandle connectionHandle, ConnectionProperties connectionProperties) {
        return null;
    }

    public enum ConnectionCacheSettings {
        /**
         * JDBC connection property for setting connection cache concurrency level.
         */
        CONCURRENCY_LEVEL(CONN_CACHE_KEY_BASE + ".concurrency", "10"),

        /**
         * JDBC connection property for setting connection cache initial capacity.
         */
        INITIAL_CAPACITY(CONN_CACHE_KEY_BASE + ".initialcapacity", "100"),

        /**
         * JDBC connection property for setting connection cache maximum capacity.
         */
        MAX_CAPACITY(CONN_CACHE_KEY_BASE + ".maxcapacity", "1000"),

        /**
         * JDBC connection property for setting connection cache expiration duration.
         */
        EXPIRY_DURATION(CONN_CACHE_KEY_BASE + ".expiryduration", "10"),

        /**
         * JDBC connection property for setting connection cache expiration unit.
         */
        EXPIRY_UNIT(CONN_CACHE_KEY_BASE + ".expiryunit", TimeUnit.MINUTES.name());

        private final String key;
        private final String defaultValue;

        ConnectionCacheSettings(String key, String defaultValue) {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        /**
         * The configuration key for specifying this setting.
         */
        public String key() {
            return key;
        }

        /**
         * The default value for this setting.
         */
        public String defaultValue() {
            return defaultValue;
        }
    }

//    private class StatementExpiryHandler
//            implements RemovalListener<Integer, StatementInfo> {
//
//        public void onRemoval(RemovalNotification<Integer, StatementInfo> notification) {
//            Integer stmtId = notification.getKey();
//            StatementInfo doomed = notification.getValue();
//            if (doomed == null) {
//                // log/throw?
//                return;
//            }
//            LOG.debug("Expiring statement {} because {}", stmtId, notification.getCause());
//            try {
//                if (doomed.getResultSet() != null) {
//                    doomed.getResultSet().close();
//                }
//                if (doomed.statement != null) {
//                    doomed.statement.close();
//                }
//            } catch (Throwable t) {
//                LOG.info("Exception thrown while expiring statement {}", stmtId, t);
//            }
//        }
//    }

    private class ConnectionExpiryHandler
            implements RemovalListener<String, Connection> {

        public void onRemoval(RemovalNotification<String, Connection> notification) {
            String connectionId = notification.getKey();
            Connection doomed = notification.getValue();
//            LOG.debug("Expiring connection {} because {}", connectionId, notification.getCause());
            try {
                if (doomed != null) {
                    doomed.close();
                }
            } catch (Throwable t) {
//                LOG.info("Exception thrown while expiring connection {}", connectionId, t);
            }
        }
    }
}
