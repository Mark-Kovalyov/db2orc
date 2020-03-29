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
        int length = typeDescription.getMaxLength();
        int precision = typeDescription.getPrecision();
        int scale = typeDescription.getScale();
        boolean isPrimitive = typeDescription.getCategory().isPrimitive();
        if (orcType.equalsIgnoreCase("STRING")) {
            res = "VARCHAR(" + length + ")";
        } else if (orcType.equalsIgnoreCase("DECIMAL")) {
            res = "DECIMAL";
        } else if (orcType.equalsIgnoreCase("DOUBLE")) {
            // TODO: Check
            res = "REAL";
        } else if (orcType.equalsIgnoreCase("DATE")) {
            res = "DATE";
        } else if (orcType.equalsIgnoreCase("BIGINT")) {
            res = "BIGINT";
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
        if (databaseType.equals("jsonb")) {
            // TODO: This is not a good idea to convert from json to Varchar. Should be discussed
            TypeDescription typeDesc = TypeDescription.createVarchar();
            return typeDesc;
        } else if (databaseType.equals("text")) {
            TypeDescription typeDesc = TypeDescription.createVarchar();
            return typeDesc;
        } else if (databaseType.equals("varchar")) {
            // TODO: Investigate for maxLength limitations
            TypeDescription typeDesc = TypeDescription.createVarchar();
            if (databaseLength != null) {
                typeDesc.withMaxLength(databaseLength);
            }
            return typeDesc;
        } else if (databaseType.equals("float8")) {
            return TypeDescription.createDouble();
        } else if (databaseType.equals("int4") || databaseType.equals("numeric")) {
            return TypeDescription.createInt();
        } else if (databaseType.equals("bpchar")) {
            return TypeDescription.createString();
        } else if (databaseType.equals("timestamptz")) {
            return TypeDescription.createTimestamp();
        } else {
            throw new RuntimeException("Unable to map database type = " + databaseType + " to ORC");
        }
    }
}
