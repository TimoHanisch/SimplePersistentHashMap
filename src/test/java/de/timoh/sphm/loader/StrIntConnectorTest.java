package de.timoh.sphm.loader;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 */
public class StrIntConnectorTest {

    private final static String dbUrl = "jdbc:postgresql://localhost/maspmemo";

    private final static String dbUser = "postgres";

    private final static String dbPw = "postgres";

    private final static String tableName = "testmap";

    private StrIntConnector instance;

    private final Map<String, Integer> map = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        ConnectorInformation connectorInformation = new ConnectorInformation(dbUrl, dbUser, dbPw, tableName);
        instance = new StrIntConnector(connectorInformation);
        instance.initialize(map);
    }

    @After
    public void tearDown() throws SQLException {
        instance.getConnectorInfo().close();
    }

    /**
     * Test of load method, of class StrIntConnector.
     *
     * @throws java.sql.SQLException
     */
    @Test
    public void testPutLoad() throws SQLException {
        System.out.println("CLEAR SQL TABLE");
        instance.forceClear();
        System.out.println("PUT/LOAD");
        assertEquals(map.size(), 0);

        instance.put("foo", 42);
        instance.load();
        assertEquals(map.size(), 1);

        instance.put("bar", 1337);
        instance.load();
        assertEquals(map.size(), 2);

        System.out.println("FORCECLEAR");

        instance.forceClear();
        instance.load();
        assertEquals(map.size(), 0);

        System.out.println("FORCESYNCHRONIZATION");
        
        map.put("foo", 42);
        map.put("bar", 1337);
        instance.load();
        assertEquals(map.size(), 0);
        
        map.put("foo", 42);
        map.put("bar", 1337);
        instance.forceSynchronization();
        assertEquals(map.size(), 2);
        
        System.out.println("REMOVE");
        instance.remove("foo");
        instance.load();
        assertEquals(map.size(), 1);
    }
}
