package com.hzh.server;

import org.apache.calcite.avatica.*;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

/**
 * @ClassName HzhJdbc40Factory
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 5:18
 * @Version 0.0.1
 **/
@SuppressWarnings("UnusedDeclaration")
public class HzhJdbc40Factory extends HzhFactory {
    public HzhJdbc40Factory() {
        this(4, 0);
    }

    protected HzhJdbc40Factory(int major, int minor) {
        super(major, minor);
    }

    @Override
    public AvaticaConnection newConnection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) {
        return new HzhJdbc40Connection(driver, factory, url, info);
    }

    @Override
    public HzhJdbc40Statement newStatement(AvaticaConnection connection,
                                           Meta.StatementHandle h,
                                           int resultSetType,
                                           int resultSetConcurrency,
                                           int resultSetHoldability) {
        return new HzhJdbc40Statement(
                (HzhConnectionImpl) connection,
                h,
                resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    @Override
    public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h, Meta.Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new HzhJdbc40PreparedStatement(
                (HzhConnectionImpl) connection, h,
                signature, resultSetType,
                resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public AvaticaResultSet newResultSet(AvaticaStatement statement, QueryState state,
                                         Meta.Signature signature, TimeZone timeZone, Meta.Frame firstFrame)
            throws SQLException {
        final ResultSetMetaData metaData =
                newResultSetMetaData(statement, signature);
        HzhConnectionImpl connection = (HzhConnectionImpl) statement.getConnection();
        return new QuicksqlResultSet(statement, signature, metaData, timeZone,
                firstFrame);
    }

    @Override
    public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
        return new HzhJdbc41DatabaseMetaDataImpl(
                (HzhConnectionImpl) connection);
    }

    @Override
    public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement, Meta.Signature signature) throws SQLException {
        return new AvaticaResultSetMetaData(statement, null, signature);
    }

    private static class HzhJdbc40Connection extends HzhConnectionImpl {
        HzhJdbc40Connection(UnregisteredDriver driver, AvaticaFactory factory, String url,
                            Properties info) {
            super(driver, factory, url, info);
        }
    }


    /**
     * Implementation of statement for JDBC 4.0.
     */
    private static class HzhJdbc40Statement extends HzhStatement {
        HzhJdbc40Statement(HzhConnectionImpl connection,
                           Meta.StatementHandle h, int resultSetType, int resultSetConcurrency,
                           int resultSetHoldability) {
            super(connection, h, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
    }


    private static class HzhJdbc40PreparedStatement
            extends HzhPreparedStatement {
        HzhJdbc40PreparedStatement(HzhConnectionImpl connection,
                                   Meta.StatementHandle h, Meta.Signature signature,
                                   int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                throws SQLException {
            super(connection, h, signature, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
    }


    private static class HzhJdbc41DatabaseMetaDataImpl
            extends AvaticaDatabaseMetaData {
        HzhJdbc41DatabaseMetaDataImpl(HzhConnectionImpl connection) {
            super(connection);
        }
    }
}
