package de.timoh.sphm;

import de.timoh.sphm.connector.SimplePersistentHashMap;
import de.timoh.sphm.connector.ConnectorInformation;
import de.timoh.sphm.connector.StrDoubleBlockingConnector;
import de.timoh.sphm.connector.StrIntBlockingConnector;
import de.timoh.sphm.connector.StrLongBlockingConnector;
import java.util.Map;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 */
public class SimplePersistentMapFactory {

    public static Map<String, Integer> createStringIntegerSimplePersistentBlockingMap(String dbUrl, String dbUser, String dbPw, String tableName) {
        ConnectorInformation connectorInformation;
        try {
            connectorInformation = new ConnectorInformation(dbUrl, dbUser, dbPw, tableName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return new SimplePersistentHashMap<>(connectorInformation, new StrIntBlockingConnector(connectorInformation));
    }
    
    public static Map<String, Long> createStringLongSimplePersistentBlockingMap(String dbUrl, String dbUser, String dbPw, String tableName) {
        ConnectorInformation connectorInformation;
        try {
            connectorInformation = new ConnectorInformation(dbUrl, dbUser, dbPw, tableName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return new SimplePersistentHashMap<>(connectorInformation, new StrLongBlockingConnector(connectorInformation));
    }

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
