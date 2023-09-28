package hzh.server;

import com.hzh.client.Driver;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @ClassName JdbcRemoteTest
 * @Description TODO
 * @Author DaHuangGo
 * @Date 2023/9/27 16:45
 * @Version 0.0.1
 **/
public class JdbcRemoteTest {
    private HttpServer jsonServer;

    @Before
    public void testJdbcServer()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, InterruptedException {
        final String[] mainArgs = new String[]{com.hzh.Main.FullyRemoteJdbcMetaFactory.class.getName()};
        jsonServer = Main.start(mainArgs, 5888, AvaticaJsonHandler::new);
        String url = Driver.CONNECT_STRING_PREFIX + "url=http://localhost:" + jsonServer.getPort();
        System.out.println(url);
        new Thread(() -> {
            try {
                jsonServer.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @After
    public void shutdown() {
        jsonServer.stop();
    }

    @Test
    public void testExecuteQuery() {
        try {
            AvaticaConnection conn = getConnection();
            final AvaticaStatement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("select * from (values (1, 'a'), (2, 'b'))");
            while (rs.next()) {
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
            }
            close(rs, statement,conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPrepareStatementQuery() {
        try {
            AvaticaConnection conn = getConnection();
            final AvaticaStatement statement = conn.createStatement();
            PreparedStatement preparedStatement = conn.prepareStatement("select * from (values (1, 'a'), (2, 'b'), "
                    + "(3, 'c')) "
                    + "where expr_col__0 = ? or expr_col__1 = ?");
            preparedStatement.setInt(1,1);
            preparedStatement.setString(2,"b");
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
            }
            close(rs, statement,conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private AvaticaConnection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.hzh.client.Driver");
        String url = "jdbc:hzh:url=http://localhost:5888";
        Properties properties = new Properties();
        properties.put("runner","jdbc");
        return (AvaticaConnection) DriverManager.getConnection(url,properties);
    }

    private void close( ResultSet rs, AvaticaStatement statement,AvaticaConnection conn) throws SQLException {
        rs.close();
        statement.close();
        conn.close();
    }
}
