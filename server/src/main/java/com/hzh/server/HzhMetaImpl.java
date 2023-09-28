package com.hzh.server;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.TypedValue;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * @ClassName HzhMetaImpl
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 5:04
 * @Version 0.0.1
 **/
public class HzhMetaImpl extends MetaImpl {
    public HzhMetaImpl(AvaticaConnection connection) {
        super(connection);
        this.connProps
                .setAutoCommit(false)
                .setReadOnly(false)
                .setTransactionIsolation(Connection.TRANSACTION_NONE);
        this.connProps.setDirty(false);
    }

    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql, long l) {
        StatementHandle result = super.createStatement(ch);
        result.signature = getConnection().mockPreparedSignature(sql);
        return result;
    }

    @SuppressWarnings("deprecation")
    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h,
                                           String sql, long maxRowCount, PrepareCallback callback) {
        return prepareAndExecute(h, sql, maxRowCount, -1, callback);
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h,
                                           String sql, long maxRowCount, int maxRowsInFirstFrame,
                                           PrepareCallback callback) {
        try {
            synchronized (callback.getMonitor()) {
                callback.clear();
                final HzhConnectionImpl connection = getConnection();
                //先使用mock数据
//                 h.signature = connection.mockPreparedSignature(sql);
                callback.assign(h.signature, null, -1);
            }
            callback.execute();
            final MetaResultSet metaResultSet =
                    MetaResultSet.create(h.connectionId, h.id, false, h.signature, null);
            return new ExecuteResult(ImmutableList.of(metaResultSet));

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // TODO: share code with prepare and createIterable
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle statementHandle, List<String> list) throws NoSuchStatementException {
        return new ExecuteBatchResult(new long[]{});
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle statementHandle, List<List<TypedValue>> list) throws NoSuchStatementException {
        return new ExecuteBatchResult(new long[]{});
    }

    @Override
    public Frame fetch(StatementHandle statementHandle, long l, int i) throws NoSuchStatementException, MissingResultsException {
        return null;
    }

    @Deprecated
    @Override
    public ExecuteResult execute(StatementHandle h,
                                 List<TypedValue> parameterValues, long maxRowCount) {
        final MetaResultSet metaResultSet = MetaResultSet.create(h.connectionId, h.id, false, h.signature, null);
        return new ExecuteResult(Collections.singletonList(metaResultSet));
    }

    @Override
    public ExecuteResult execute(StatementHandle h,
                                 List<TypedValue> parameterValues, int maxRowsInFirstFrame) {
        final MetaResultSet metaResultSet = MetaResultSet.create(h.connectionId, h.id, false, h.signature, null);
        return new ExecuteResult(Collections.singletonList(metaResultSet));
    }

    @Override
    public void closeStatement(StatementHandle statementHandle) {

    }

    @Override
    public boolean syncResults(StatementHandle statementHandle, QueryState queryState, long l) throws NoSuchStatementException {
        return false;
    }

    @Override
    public void commit(ConnectionHandle connectionHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(ConnectionHandle connectionHandle) {
        throw new UnsupportedOperationException();
    }

    HzhConnectionImpl getConnection() {
        return (HzhConnectionImpl) connection;
    }
}
