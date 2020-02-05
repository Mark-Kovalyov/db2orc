package mayton.db.h2;

import mayton.db.TypeMapper;

/**
 * <pre>
 * INT
 * BOOLEAN
 * TINYINT
 * SMALLINT
 * BIGINT
 * IDENTITY
 * DECIMAL
 * DOUBLE
 * REAL
 * TIME
 * TIME WITH TIME ZONE
 * DATE
 * TIMESTAMP
 * TIMESTAMP WITH TIME ZONE
 * BINARY
 * OTHER
 * VARCHAR
 * VARCHAR_IGNORECASE
 * CHAR
 * BLOB
 * CLOB
 * UUID
 * ARRAY
 * ENUM
 * GEOMETRY
 * JSON
 * INTERVAL
 * </pre>
 */
public class H2TypeMapper extends TypeMapper {

    @Override
    public String fromOrc(String orcType, Integer length, Integer precision) {
        if (orcType.equalsIgnoreCase("STRING")) {
            return "VARCHAR";
        } else if (orcType.equalsIgnoreCase("DECIMAL")) {
            return "DECIMAL";
        } else if (orcType.equalsIgnoreCase("DOUBLE")) {
            return "DOUBLE";
        } else {
            throw new RuntimeException("Unable to map " + orcType);
        }
    }
}
