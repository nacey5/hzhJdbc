package com.hzh.server;

import org.apache.calcite.avatica.*;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @ClassName HzhConnectionImpl
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 4:58
 * @Version 0.0.1
 **/
public class HzhConnectionImpl extends AvaticaConnection {
    protected HzhConnectionImpl(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) {
        super(driver, factory, url, info);
    }

    @Override
    public AvaticaStatement createStatement() throws SQLException {
        return super.createStatement();
    }

    @Override public HzhStatement createStatement(int resultSetType,
                                                       int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return (HzhStatement) super.createStatement(resultSetType,
                resultSetConcurrency, resultSetHoldability);
    }

    //justMock,not real Data
    public Meta.Signature mockPreparedSignature(String sql) {
        List<AvaticaParameter> params = new ArrayList<AvaticaParameter>();
        int startIndex = 0;
        while (sql.indexOf("?", startIndex) >= 0) {
            AvaticaParameter param = new AvaticaParameter(false, 0, 0, 0, null, null, null);
            params.add(param);
            startIndex = sql.indexOf("?", startIndex) + 1;
        }

        ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
        Map<String, Object> internalParams = Collections.<String, Object> emptyMap();

        return new Meta.Signature(columns, sql, params, internalParams, Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT);
    }
}
