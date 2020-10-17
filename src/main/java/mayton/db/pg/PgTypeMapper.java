package mayton.db.pg;

import mayton.db.OrcTypes;
import mayton.db.GenericTypeMapper;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

@ThreadSafe
public class PgTypeMapper extends GenericTypeMapper {

    static Logger logger = LogManager.getLogger(PgTypeMapper.class);

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
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, Optional<Integer> dataTypeLength, Optional<Integer> dataTypeScale, boolean isNullable) {
        logger.trace(":: toOrc dt = {}, dtl = {}, dts = {}, isNul = {}",
                databaseType,
                dataTypeLength.isPresent() ? String.valueOf(dataTypeLength.get()) : "?",
                dataTypeScale.isPresent() ? String.valueOf(dataTypeScale.get()) : "?",
                isNullable);

        if (databaseType.equalsIgnoreCase(PgTypes.JSONB.name())) {
            // TODO: This is not a good idea to convert from json to string. Should be discussed
            TypeDescription typeDesc = TypeDescription.createString();
            return typeDesc;
        } else if (databaseType.equalsIgnoreCase(PgTypes.NUMERIC.name())) {
            TypeDescription typeDesc = TypeDescription.createDecimal();
            if (dataTypeLength.isPresent()) typeDesc = typeDesc.withPrecision(dataTypeLength.get());
            if (dataTypeScale.isPresent()) typeDesc = typeDesc.withScale(dataTypeScale.get());
            return typeDesc;
        } else if (databaseType.equalsIgnoreCase(PgTypes.TEXT.name())) {
            return TypeDescription.createString();
        } else if (databaseType.equalsIgnoreCase(PgTypes.VARCHAR.name())) {
            // TODO: Investigate for maxLength limitations
            TypeDescription typeDesc = TypeDescription.createVarchar();
            if (dataTypeLength.isPresent()) {
                typeDesc = typeDesc.withMaxLength(dataTypeLength.get());
            }
            return typeDesc;
        } else if (databaseType.equalsIgnoreCase(PgTypes.FLOAT8.name())) {
            return TypeDescription.createDouble();
        } else if (databaseType.equalsIgnoreCase(PgTypes.INT4.name()) || databaseType.equalsIgnoreCase(PgTypes.SERIAL.name())) {
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

    @Override
    public void toOrcVectorized(@NotNull VectorizedRowBatch batch, int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            String columnClassName = rsmd.getColumnClassName(i + 1);
            String columnTypeName = rsmd.getColumnTypeName(i + 1);
            Object sqlFieldValue = resultSet.getObject(i + 1);
            if (sqlFieldValue != null) {
                // TODO: PostgreSQL specific mapping
                if (columnTypeName.equals(PgTypes.BPCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.VARCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.TEXT.name().toLowerCase())) {
                    ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, ((String) sqlFieldValue).getBytes(UTF_8));
                } else if (columnTypeName.equals(PgTypes.INT4.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.SERIAL.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = ((int) sqlFieldValue);
                } else if (columnTypeName.equals(PgTypes.NUMERIC.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = ((BigDecimal) sqlFieldValue).longValue();
                } else if (columnTypeName.equals(PgTypes.FLOAT8.name().toLowerCase())) {
                    ((DoubleColumnVector) batch.cols[i]).vector[rowInBatch] = (double) sqlFieldValue;
                } else if (columnTypeName.equals(PgTypes.TIMESTAMPTZ.name().toLowerCase())) {
                    ((TimestampColumnVector) batch.cols[i]).set(rowInBatch, (Timestamp) sqlFieldValue);
                } else if (columnTypeName.equals(PgTypes.JSONB.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.POINT.name().toLowerCase())) {
                    // TODO: Stuped JSONB/POINT stub
                    ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, "{}".getBytes(UTF_8));
                } else {
                    throw new RuntimeException("Unable to append row for columnClassName = " +
                            columnClassName + " columnTypeName = " + columnTypeName +
                            " during vectorized row batch generation, value = " + sqlFieldValue);
                }
            } else {
                // TODO: Stuped null replacement! Rework.
                if (columnTypeName.equals(PgTypes.BPCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.VARCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.TEXT.name().toLowerCase())) {
                    ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, EMPTY_BYTE_ARRAY);
                } else if (columnTypeName.equals(PgTypes.INT4.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = Integer.MIN_VALUE;
                } else if (columnTypeName.equals(PgTypes.NUMERIC.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = Long.MIN_VALUE;
                } else if (columnTypeName.equals(PgTypes.FLOAT8.name().toLowerCase())) {
                    ((DoubleColumnVector) batch.cols[i]).vector[rowInBatch] = Double.NaN;
                }
            }
        }
    }
}
