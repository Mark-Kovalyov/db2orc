package mayton.db.pg;

import mayton.db.OrcTypes;
import mayton.db.GenericTypeMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class PGGenericType extends GenericTypeMapper {

    static Logger logger = LogManager.getLogger(PGGenericType.class);

    @Override
    public String fromOrc(@NotNull TypeDescription typeDescription) {

        String orcType = typeDescription.getCategory().getName();
        String res = "";
        int length = typeDescription.getMaxLength();
        int precision = typeDescription.getPrecision();
        int scale = typeDescription.getScale();
        boolean isPrimitive = typeDescription.getCategory().isPrimitive();
        if (orcType.equalsIgnoreCase(OrcTypes.STRING.name())) {
            res = "VARCHAR(" + length + ")";
        } else if (orcType.equalsIgnoreCase(OrcTypes.DECIMAL.name())) {
            res = "DECIMAL";
        } else if (orcType.equalsIgnoreCase(OrcTypes.DOUBLE.name())) {
            // TODO: Check
            res = "REAL";
        } else if (orcType.equalsIgnoreCase(OrcTypes.DATE.name())) {
            res = "DATE";
        } else if (orcType.equalsIgnoreCase(OrcTypes.BIGINT.name())) {
            res = "BIGINT";
        } else if (orcType.equalsIgnoreCase(OrcTypes.ARRAY.name())) {
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
        if (databaseType.equalsIgnoreCase(PgTypes.JSONB.name())) {
            // TODO: This is not a good idea to convert from json to string. Should be discussed
            TypeDescription typeDesc = TypeDescription.createString();
            return typeDesc;
        } else if (databaseType.equalsIgnoreCase(PgTypes.TEXT.name())) {
            return TypeDescription.createString();
        } else if (databaseType.equalsIgnoreCase(PgTypes.VARCHAR.name())) {
            // TODO: Investigate for maxLength limitations
            TypeDescription typeDesc = TypeDescription.createVarchar();
            if (databaseLength != null) {
                typeDesc = typeDesc.withMaxLength(databaseLength);
            }
            return typeDesc;
        } else if (databaseType.equalsIgnoreCase(PgTypes.FLOAT8.name())) {
            return TypeDescription.createDouble();
        } else if (databaseType.equalsIgnoreCase(PgTypes.INT4.name()) || databaseType.equalsIgnoreCase(PgTypes.NUMERIC.name()) || databaseType.equalsIgnoreCase(PgTypes.SERIAL.name())) {
            return TypeDescription.createInt();
        } else if (databaseType.equalsIgnoreCase(PgTypes.BPCHAR.name())) {
            return TypeDescription.createString();
        } else if (databaseType.equalsIgnoreCase(PgTypes.POINT.name())) {
            // TODO: This is not a good idea to convert from point to string. Should be discussed
            return TypeDescription.createString();
        } else if (databaseType.equalsIgnoreCase(PgTypes.TIMESTAMPTZ.name())) {
            return TypeDescription.createTimestamp();
        } else {
            throw new RuntimeException("Unable to map database type = " + databaseType + " to ORC during header generation");
        }
    }
}
