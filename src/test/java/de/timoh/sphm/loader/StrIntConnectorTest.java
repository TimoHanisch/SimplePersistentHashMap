package de.timoh.sphm.loader;

import java.util.HashMap;
import java.util.Map;
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

    /**
     * Test of all implemented functions
     *
     * @throws java.sql.SQLException
     */
    @Test
    public void testPutLoad() throws Exception {
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

        instance.forceClear();
        
        int uniqueInserts = 100000;
        long time = System.nanoTime();
        for (int i = 0; i < uniqueInserts; i++) {
            map.put("Entry" + i, i);
        }
        System.out.println("Standard HashMap took " + ((System.nanoTime() - time) / 1000000000.) + "seconds for " + uniqueInserts + " puts");
        
        map.clear();
        instance.forceClear();
        
        time = System.nanoTime();
        for (int i = 0; i < uniqueInserts; i++) {
            map.put("Entry" + i, i);
        }
        instance.forceSynchronization();
        System.out.println("MapConnector took " + ((System.nanoTime() - time) / 1000000000.) + "seconds for " + uniqueInserts + " puts");
        
        instance.forceDelete();
    }
}
