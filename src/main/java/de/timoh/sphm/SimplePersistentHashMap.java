package de.timoh.sphm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SimplePersistentHashMap extends HashMap<String, Integer> {

	private static final Log LOG = LogFactory.getLog(SimplePersistentHashMap.class);

	private static final long serialVersionUID = -3452372538801457617L;

	private static Connection con;

	public SimplePersistentHashMap(String url, String user, String password) {
		super();
		if (con == null) {
			try {
				con = DriverManager.getConnection(url, user, password);
				if (con == null) {
					System.out.println("NULL");
				} else {
					load();
				}
			} catch (SQLException e) {
				LOG.error(e);
			}
		}
	}

	private void load() {
		String stm = "SELECT * FROM persistent_map";
		String key;
		int value;
		try {
			PreparedStatement pStm = con.prepareStatement(stm);
			ResultSet resultSet = pStm.executeQuery();
			while (resultSet.next()) {
				key = resultSet.getString("key");
				value = resultSet.getInt("count");
				super.put(key, value);
			}
		} catch (SQLException e) {
			LOG.error(e);
		}
	}

	@Override
	public Integer put(String key, Integer value) {
		String stm;
		if (super.containsKey(key)) {
			stm = "SELECT insertIfExists('" + key + "', " + value + ");";
		} else {
			stm = "INSERT INTO persistent_map(key,count) VALUES ('" + key + "'," + value + ");";
		}
		try {
			PreparedStatement pStm = con.prepareStatement(stm);
			if (!pStm.execute()) {
				LOG.warn("Persistency not guaranteed, since key-value pair could not be inserted");
			}
		} catch (SQLException e) {
			LOG.error(e);
			return null;
		}
		return super.put(key, value);
	}

	@Override
	public Integer remove(Object key) {
		if (super.containsKey(key)) {
			String stm = "DELETE FROM persistent_map WHERE key = '" + key + "'";
			try {
				if (!con.prepareStatement(stm).execute()) {
					LOG.warn("Persistency not guaranteed, since key-value pair could not be deleted");
				}
			} catch (SQLException e) {
				LOG.error(e);
			}
		}
		return super.remove(key);
	}

	/**
	 * 
	 */
	public void reload() {
		super.clear();
		load();
	}

	/**
	 * 
	 */
	public void forceSynchronization() {
		try {
			for (String s : super.keySet()) {
				String stm = "SELECT insertIfExists('" + s + "', " + super.get(s) + ");";
				if (!con.prepareStatement(stm).execute()) {
					LOG.warn("Persistency not guaranteed, since key-value pair could not be inserted");
				}
			}
		} catch (SQLException e) {
			LOG.error(e);
		}
	}

	public void close() {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				LOG.error(e);
			}
		}
	}
}
