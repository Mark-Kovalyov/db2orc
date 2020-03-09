package mayton.db.pg;

import mayton.db.TypeMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public class PGTypeMapper extends TypeMapper {

    enum PGTypes {
        STRING, DECIMAL, DOUBLE, DATE, BIGINT, TEXT
    }

    static Logger logger = LogManager.getLogger(PGTypeMapper.class);

    @Override
    public String fromOrc(@NotNull TypeDescription typeDescription) {

        String orcType = typeDescription.getCategory().getName();
        String res = "";
        int length    = typeDescription.getMaxLength();
        int precision = typeDescription.getPrecision();
        int scale     = typeDescription.getScale();
        boolean isPrimitive = typeDescription.getCategory().isPrimitive();
        if (orcType.equalsIgnoreCase("STRING")) {
            res = "VARCHAR(" + length +")";
        } else if (orcType.equalsIgnoreCase("DECIMAL")) {
            res = "DECIMAL";
        } else if (orcType.equalsIgnoreCase("DOUBLE")) {
            // TODO: Check
            res = "REAL";
        } else if (orcType.equalsIgnoreCase("DATE")) {
            res = "DATE";
        } else if (orcType.equalsIgnoreCase("BIGINT")) {
            res =  "BIGINT";
        } else if (orcType.equalsIgnoreCase("ARRAY")) {
            // TODO: Wtf? ListColumnVector
            logger.info("ARRAY attributes {}", typeDescription.getAttributeNames());
            res = "TEXT[]";
        } else {
            logger.error("orcType = {} len = {}, prec = {}, scale = {}, prim = {} mapped into {}", orcType, length, precision, scale, isPrimitive, res);
            throw new RuntimeException("Unable to map " + orcType);
        }
        logger.info("{} / len = {}, prec = {}, scale = {}, prim = {} mapped into {}", typeDescription.toJson(), length, precision, scale, isPrimitive, res);
        return res;
    }

    @Override
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer databaseLength, @Nullable Integer databasePrecision, boolean isNullable) {
        return TypeDescription.createVarchar().withMaxLength(255);
    }
}
