package mayton.db.h2;

import mayton.db.TypeMapper;
import org.apache.orc.TypeDescription;

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
    public String fromOrc(TypeDescription typeDescription) {
        String orcType = typeDescription.getCategory().getName();
        if (orcType.equalsIgnoreCase("STRING")) {
            int length = typeDescription.getMaxLength();
            int precision = typeDescription.getPrecision();
            return "VARCHAR(" + length +")";
        } else if (orcType.equalsIgnoreCase("DECIMAL")) {
            return "DECIMAL";
        } else if (orcType.equalsIgnoreCase("DOUBLE")) {
            return "DOUBLE";
        } else if (orcType.equalsIgnoreCase("date")) {
            return "DATE";
        } else {
            throw new RuntimeException("Unable to map " + orcType);
        }
    }
}
