CREATE OR REPLACE FUNCTION insertIfExistsStrInt(k varchar(255),v integer) RETURNS integer AS $$
	BEGIN 
		DELETE FROM persistent_map WHERE key = k;
		INSERT INTO persistent_map(key,count) VALUES (k,v);
		RETURN v;
	END;
	$$ LANGUAGE plpgsql;