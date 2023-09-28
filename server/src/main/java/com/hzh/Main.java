package com.hzh;

import com.hzh.server.Driver;
import com.hzh.server.HzhServerMeta;
import com.hzh.server.OptionsParser;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws ParseException, ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException, UnknownHostException, InterruptedException {
        OptionsParser parser = new OptionsParser(args);
        final int port = Integer.parseInt(parser.getOptionValue(OptionsParser.SubmitOption.PORT));
        final String[] mainArgs = new String[]{FullyRemoteJdbcMetaFactory.class.getName()};

        HttpServer jsonServer = org.apache.calcite.avatica.server.Main.start(mainArgs, port, service -> new AvaticaJsonHandler(service));
        InetAddress address = InetAddress.getLocalHost();
        String hostName = "localhost";
        if (address != null) {
            hostName = StringUtils.isNotBlank(address.getHostName()) ? address.getHostName() : address
                    .getHostAddress();
        }
        String url = Driver.CONNECT_STRING_PREFIX + "url=http://" + hostName + ":" + jsonServer.getPort();
        System.out.println("Hzh server started, Please connect : " + url);
        jsonServer.join();
    }


    public static class FullyRemoteJdbcMetaFactory implements Meta.Factory {

        private static HzhServerMeta instance = null;

        private static HzhServerMeta getInstance() {
            if (instance == null) {
                try {
                    Class.forName("com.hzh.server.Driver");
                    instance = new HzhServerMeta("jdbc:hzh:server:");
                }  catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            return instance;
        }

        @Override
        public Meta create(List<String> args) {
            return getInstance();
        }
    }
}