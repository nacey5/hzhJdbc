package com.hzh.server;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;

/**
 * @ClassName HzhStatement
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 5:01
 * @Version 0.0.1
 **/
public abstract class HzhStatement extends AvaticaStatement {

    /**
     * Creates a HzhStatement.
     *
     * @param connection Connection
     * @param h Statement handle
     * @param resultSetType Result set type
     * @param resultSetConcurrency Result set concurrency
     * @param resultSetHoldability Result set holdability
     */
    HzhStatement(HzhConnectionImpl connection, Meta.StatementHandle h,
                      int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        super(connection, h, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }
}
