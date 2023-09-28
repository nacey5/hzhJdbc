package com.hzh.server;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
/**
 * @ClassName Driver
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 4:50
 * @Version 0.0.1
 **/
public class Driver extends UnregisteredDriver{
    public static final String CONNECT_STRING_PREFIX = "jdbc:hzh:";
    private static final String METADATA_PROPERTIES_RESOURCE_PATH = "hzh-jdbc.properties";

    static {
        new Driver().register();
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DriverVersion.load(
                Driver.class,
                METADATA_PROPERTIES_RESOURCE_PATH,
                "hzj JDBC Driver",
                "unknown version",
                "hzh-jdbc",
                "<Properties resource " + METADATA_PROPERTIES_RESOURCE_PATH + " not loaded>");
    }

    @Override
    protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        switch (jdbcVersion) {
            case JDBC_30:
                throw new IllegalArgumentException("JDBC version not supported: " + jdbcVersion);
            case JDBC_40:
                //jdbc40factory 注册
                return HzhJdbc40Factory.class.getName();
            case JDBC_41:
            default:
                // jdbc40factory 注册
                return HzhJdbc40Factory.class.getName();
        }
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    public Meta createMeta(AvaticaConnection connection) {
        return new HzhMetaImpl((HzhConnectionImpl)connection);
    }
}
