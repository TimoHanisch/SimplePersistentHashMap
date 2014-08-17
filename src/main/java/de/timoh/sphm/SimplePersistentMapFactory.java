package de.timoh.sphm;

import de.timoh.sphm.connector.ConnectorInformation;
import de.timoh.sphm.connector.StrIntBlockingConnector;
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
    
}
