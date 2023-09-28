package com.hzh.server;

import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;

/**
 * @ClassName QuicksqlResultSet
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 13:40
 * @Version 0.0.1
 **/
public class QuicksqlResultSet extends AvaticaResultSet {

    /**
     * Creates a QuicksqlResultSet.
     */
    public QuicksqlResultSet(AvaticaStatement statement,
                             Meta.Signature calciteSignature,
                             ResultSetMetaData resultSetMetaData, TimeZone timeZone,
                             Meta.Frame firstFrame) throws SQLException {
        super(statement, null, calciteSignature, resultSetMetaData, timeZone, firstFrame);
    }
    @Override
    protected AvaticaResultSet execute() throws SQLException {
        return this;
    }

    public static class QueryResult {

        public final List<ColumnMetaData> columnMeta;
        public final Iterable<Object> iterable;

        public QueryResult(List<ColumnMetaData> columnMeta, Iterable<Object> iterable) {
            this.columnMeta = columnMeta;
            this.iterable = iterable;
        }
    }
}

// End QuicksqlResultSet.java
