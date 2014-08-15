/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

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
     * @throws java.sql.SQLException
     */
    @Test
    public void testLoad() throws SQLException {
        System.out.println("load");
        instance.load();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of forceSynchronization method, of class StrIntConnector.
     */
    @Test
    public void testForceSynchronization() throws SQLException {
        System.out.println("forceSynchronization");
        instance.forceSynchronization();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of put method, of class StrIntConnector.
     */
    @Test
    public void testPut() throws SQLException {
        System.out.println("put");
        String key = "";
        Integer value = null;
        instance.put(key, value);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of remove method, of class StrIntConnector.
     */
    @Test
    public void testRemove() throws SQLException {
        System.out.println("remove");
        String key = "";
        Integer expResult = null;
        Integer result = instance.remove(key);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
