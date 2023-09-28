package com.hzh.server;

import org.apache.calcite.avatica.*;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

/**
 * @ClassName HzhFactory
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 5:19
 * @Version 0.0.1
 **/
public abstract class HzhFactory implements AvaticaFactory {


    protected final int major;
    protected final int minor;


    protected HzhFactory(int major, int minor) {
        this.major = major;
        this.minor = minor;
    }

    @Override
    public int getJdbcMajorVersion() {
        return major;
    }

    @Override
    public int getJdbcMinorVersion() {
        return minor;
    }

    public abstract AvaticaConnection newConnection(UnregisteredDriver driver,
                                                    AvaticaFactory factory, String url, Properties info);
}
