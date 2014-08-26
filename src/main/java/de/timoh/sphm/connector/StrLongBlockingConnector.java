package de.timoh.sphm.connector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 */
public class StrLongBlockingConnector extends MapConnector<String, Long> {

    public StrLongBlockingConnector(ConnectorInformation connectorInfo) {
        super(connectorInfo);
    }

    @Override
    public MapConnector<String, Long> load(Map<String, Long> map) throws Exception {
        getMap().clear();
        String stm = "SELECT * FROM " + getConnectorInfo().getTableName();
        String key;
        Long value;
        try (Connection con = getConnectorInfo().getConnection(); PreparedStatement pStm = con.prepareStatement(stm); ResultSet resultSet = pStm.executeQuery();) {
            while (resultSet.next()) {
                key = resultSet.getString("key");
                value = resultSet.getLong("value");
                map.put(key, value);
            }
        }
        return this;
    }

    @Override
    public MapConnector<String, Long> forceSynchronization() throws Exception {
        try (Connection con = getConnectorInfo().getConnection()) {
            forceClear();
            int n = 0;
            StringBuilder values = new StringBuilder();
            String stm;
            for (String s : getMap().keySet()) {
                values = values.append("(").append("'").append(s).append("'").append(",").append(getMap().get(s)).append(")");
                if (n + 1 != BLOCK_INSERT_COUNT && n + 1 != getMap().size() - 1) {
                    values = values.append(",");
                }
                n++;
                if (n == BLOCK_INSERT_COUNT || n == getMap().size() - 1) {
                    stm = "INSERT INTO " + getConnectorInfo().getTableName() + "(key,value) VALUES " + values.toString() + ";";
                    try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                        prepStm.execute();
                    }
                    values = new StringBuilder();
                    n = 0;
                }
            }
        }
        return this;
    }

    @Override
    public MapConnector<String, Long> initialize(Map<String, Long> map) throws Exception {
        setMap(map);
        try (Connection con = getConnectorInfo().getConnection()) {
            try (PreparedStatement prepStm = con.prepareStatement(getInsertIfExsists(getConnectorInfo().getTableName()))) {
                prepStm.execute();
            }
            if (!getConnectorInfo().tableExists()) {
                try (PreparedStatement prepStm = con.prepareStatement(getCreateTable(getConnectorInfo().getTableName()))) {
                    prepStm.execute();
                }
            }
        }
        return this;
    }

    @Override
    public MapConnector<String, Long> put(String key, Long value) throws Exception {
        String stm;
        if (getMap().containsKey(key)) {
            stm = "SELECT insertIfExistsStrLong('" + key + "', " + value + ");";
        } else {
            stm = "INSERT INTO " + getConnectorInfo().getTableName() + "(key,value) VALUES ('" + key + "'," + value + ");";
        }
        try (Connection con = getConnectorInfo().getConnection(); PreparedStatement prepStm = con.prepareStatement(stm)) {
            prepStm.execute();
        }
        return this;
    }

    @Override
    public MapConnector<String, Long> putAll(Map<? extends String, ? extends Long> map) throws Exception {
        String stm;
        try (Connection con = getConnectorInfo().getConnection()) {
            stm = getCreateTableTmp(getConnectorInfo().getTableName());
            try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                prepStm.execute();
            }
            int n = 0;
            StringBuilder values = new StringBuilder();
            for (String s : getMap().keySet()) {
                if (n == BLOCK_INSERT_COUNT || n == getMap().size() - 1) {
                    stm = "INSERT INTO " + getConnectorInfo().getTableName() + "tmp(key,value) VALUES " + values.toString() + ";";
                    try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                        prepStm.execute();
                    }
                    values = new StringBuilder();
                    n = 0;
                }
                values = values.append("(").append("'").append(s).append("'").append(",").append(getMap().get(s)).append(")");
                if (n + 1 != BLOCK_INSERT_COUNT && n + 1 != getMap().size() - 1) {
                    values = values.append(",");
                }
                n++;
            }
            stm = getMergeTables(getConnectorInfo().getTableName(), getConnectorInfo().getTableName() + "tmp");
            try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                prepStm.execute();
            }
            stm = "DROP TABLE " + getConnectorInfo().getTableName() + "tmp";
            try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                prepStm.execute();
            }
        }
        return this;
    }

    @Override
    public MapConnector<String, Long> remove(String key) throws Exception {
        if (getMap().containsKey(key)) {
            String stm = "DELETE FROM " + getConnectorInfo().getTableName() + " WHERE key = '" + key + "'";
            try (Connection con = getConnectorInfo().getConnection(); PreparedStatement prepStm = con.prepareStatement(stm)) {
                prepStm.execute();
            }
        }
        return this;
    }

    private static String getMergeTables(String table1, String table2) {
        return "INSERT INTO " + table1 + "\n"
                + "SELECT * FROM " + table2 + " \n"
                + "    WHERE NOT EXISTS(\n"
                + "            SELECT * FROM " + table1 + " \n"
                + "                 WHERE key=" + table2 + ".key \n"
                + "                       AND value=" + table2 + ".value \n"
                + "                     )";
    }

    private static String getCreateTable(String tableName) {
        return "CREATE TABLE " + tableName + "(\n"
                + "key varchar(255) PRIMARY KEY,\n"
                + "value bigint\n"
                + ");";
    }

    private static String getCreateTableTmp(String tableName) {
        return "CREATE TABLE " + tableName + "tmp (\n"
                + "key varchar(255) PRIMARY KEY,\n"
                + "value bigint\n"
                + ");";
    }

    private static String getInsertIfExsists(String tableName) {
        return "CREATE OR REPLACE FUNCTION insertIfExistsStrLong(k varchar(255),v bigint) \n"
                + "        RETURNS bigint AS $$\n"
                + "	BEGIN \n"
                + "		DELETE FROM " + tableName + " WHERE key = k;\n"
                + "		INSERT INTO " + tableName + "(key,value) VALUES (k,v);\n"
                + "		RETURN v;\n"
                + "	END;\n"
                + "	$$ LANGUAGE plpgsql;";
    }
}
