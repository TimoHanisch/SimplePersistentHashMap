package de.timoh.sphm;

import de.timoh.sphm.loader.ConnectorInformation;
import de.timoh.sphm.loader.StrIntBlockingConnector;
import java.util.Map;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 */
public class SimplePersistentMapFactory {

    public static Map<String, Integer> createStringIntegerSimplePersistentMap(String dbUrl, String dbUser, String dbPw, String tableName) {
        ConnectorInformation connectorInformation;
        try {
            connectorInformation = new ConnectorInformation(dbUrl, dbUser, dbPw, tableName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return new SimplePersistentHashMap<>(connectorInformation, new StrIntBlockingConnector(connectorInformation));
    }
    
}
