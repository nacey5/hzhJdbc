package com.hzh.server;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;

import java.sql.SQLException;

/**
 * @ClassName HzhPreparedStatement
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 13:28
 * @Version 0.0.1
 **/
public abstract class HzhPreparedStatement  extends AvaticaPreparedStatement {

    protected HzhPreparedStatement(HzhConnectionImpl connection,
                                        Meta.StatementHandle h, Meta.Signature signature, int resultSetType,
                                        int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        super(connection, h, signature, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    @Override public HzhConnectionImpl getConnection() throws SQLException {
        return (HzhConnectionImpl) super.getConnection();
    }
}
