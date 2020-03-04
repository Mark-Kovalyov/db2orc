package mayton.db.h2;

import mayton.db.TypeMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    static Logger logger = LogManager.getLogger(H2TypeMapper.class);

    @Override
    public String fromOrc(TypeDescription typeDescription) {
        logger.trace(":: typeDesc = {}", typeDescription.toJson());
        String orcType = typeDescription.getCategory().getName();
        if (orcType.equalsIgnoreCase("STRING")) {
            int length = typeDescription.getMaxLength();
            int precision = typeDescription.getPrecision();
            logger.trace(":: len = {}, prec = {}", length, precision);
            return "VARCHAR(" + length +")";
        } else if (orcType.equalsIgnoreCase("DECIMAL")) {
            return "DECIMAL";
        } else if (orcType.equalsIgnoreCase("DOUBLE")) {
            return "DOUBLE";
        } else if (orcType.equalsIgnoreCase("DATE")) {
            return "DATE";
        } else if (orcType.equalsIgnoreCase("BIGINT")) {
            return "BIGINT";
        } else {
            throw new RuntimeException("Unable to map " + orcType);
        }
    }

    @Override
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer databaseLength, @Nullable Integer databasePrecision, boolean isNullable) {
        return super.toOrc(databaseType, databaseLength, databasePrecision, isNullable);
    }
}
