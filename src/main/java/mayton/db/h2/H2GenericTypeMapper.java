package mayton.db.h2;

import mayton.db.GenericTypeMapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

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
public class H2GenericTypeMapper extends GenericTypeMapper {

    static Logger logger = LogManager.getLogger(H2GenericTypeMapper.class);

    @Override
    public String fromOrc(@NotNull TypeDescription typeDescription) {
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
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, Optional<Integer> databaseLength, Optional<Integer> databasePrecision, boolean isNullable) {
        return super.toOrc(databaseType, databaseLength, databasePrecision, isNullable);
    }

    @Override
    public void toOrcVectorized(@NotNull VectorizedRowBatch batch, int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        super.toOrcVectorized(batch, rowInBatch, resultSet);
    }


}
