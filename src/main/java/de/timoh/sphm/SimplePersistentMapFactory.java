package de.timoh.sphm;

import de.timoh.sphm.connector.ConnectorInformation;
import de.timoh.sphm.connector.StrDoubleBlockingConnector;
import de.timoh.sphm.connector.StrIntBlockingConnector;
import de.timoh.sphm.connector.StrLongBlockingConnector;
import java.util.Map;

/**
 * Offers some methods to create persistent maps. Not all types are supported.
 * 
 * @author <a href="mailto:timohanisch@gmail.com">Timo Hanisch</a>
 */
public class SimplePersistentMapFactory {

    /**
     * 
     * @param dbUrl - The database URL
     * @param dbUser - The database User
     * @param dbPw - The database Password
     * @param tableName - The database tablename
     * @return 
     */
    public static Map<String, Integer> createStringIntegerSimplePersistentBlockingMap(String dbUrl, String dbUser, String dbPw, String tableName) {
        ConnectorInformation connectorInformation;
        try {
            connectorInformation = new ConnectorInformation(dbUrl, dbUser, dbPw, tableName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return new SimplePersistentHashMap<>(connectorInformation, new StrIntBlockingConnector(connectorInformation));
    }
    
    /**
     * 
     * @param dbUrl - The database URL
     * @param dbUser - The database User
     * @param dbPw - The database Password
     * @param tableName - The database tablename
     * @return 
     */    
    public static Map<String, Long> createStringLongSimplePersistentBlockingMap(String dbUrl, String dbUser, String dbPw, String tableName) {
        ConnectorInformation connectorInformation;
        try {
            connectorInformation = new ConnectorInformation(dbUrl, dbUser, dbPw, tableName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return new SimplePersistentHashMap<>(connectorInformation, new StrLongBlockingConnector(connectorInformation));
    }
    
    /**
     * 
     * @param dbUrl - The database URL
     * @param dbUser - The database User
     * @param dbPw - The database Password
     * @param tableName - The database tablename
     * @return 
     */ 
    public static Map<String, Double> createStringDoubleSimplePersistentBlockingMap(String dbUrl, String dbUser, String dbPw, String tableName) {
        ConnectorInformation connectorInformation;
        try {
            connectorInformation = new ConnectorInformation(dbUrl, dbUser, dbPw, tableName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return new SimplePersistentHashMap<>(connectorInformation, new StrDoubleBlockingConnector(connectorInformation));
    }
}
