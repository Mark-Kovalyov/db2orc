package mayton.db.pg;

import mayton.db.OrcTypes;
import mayton.db.GenericTypeMapper;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.concurrent.ThreadSafe;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

@ThreadSafe
public class PgTypeMapper extends GenericTypeMapper {

    static Logger logger = LogManager.getLogger(PgTypeMapper.class);

    static final HiveDecimalWritable HIVE_DECIMAL_WRITABLE = new HiveDecimalWritable(HiveDecimal.create(BigInteger.ZERO));

    @Override
    @NotNull
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

    //  Column |       Type       | Collation | Nullable |              Default              | Storage | Stats target | Description
    //--------+------------------+-----------+----------+-----------------------------------+---------+--------------+-------------
    // i4     | integer          |           |          |                                   | plain   |              |
    // se     | integer          |           | not null | nextval('test5_se_seq'::regclass) | plain   |              |
    // ss     | smallint         |           | not null | nextval('test5_ss_seq'::regclass) | plain   |              |
    // bs     | bigint           |           | not null | nextval('test5_bs_seq'::regclass) | plain   |              |
    // si     | smallint         |           |          |                                   | plain   |              |
    // bi     | bigint           |           |          |                                   | plain   |              |
    // i      | integer          |           |          |                                   | plain   |              |
    // f8     | double precision |           |          |                                   | plain   |              |
    // r      | real             |           |          |                                   | plain   |              |
    //
    // Rules:
    //    precision = [1..38]
    //    scale <= precision
    @Override
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer dataTypeLength, @Nullable Integer dataTypeScale, boolean isNullable) {
        logger.trace(":: toOrc databaseType = {}, dataTypeLength/precision = {}, dataTypeScale = {}, isNullable = {}",
                databaseType,
                dataTypeLength != null ? String.valueOf(dataTypeLength) : "?",
                dataTypeScale != null ? String.valueOf(dataTypeScale) : "?",
                isNullable);

        if (databaseType.equalsIgnoreCase(PgTypes.JSONB.name())) {
            // TODO: This is not a good idea to convert from json to string. Should be discussed
            return TypeDescription.createString();
        } else if (databaseType.equalsIgnoreCase(PgTypes.NUMERIC.name())) {
            // 2020-10-29 21:06:58 [TRACE] 1   : mayton.db.Db2Orc [3.1] columnName yearstarted : dataType = 2, columnSize = 4, scale = 0, typeName = numeric, nullAllowred = 1
            // 2020-10-29 21:06:58 [TRACE] 1   : mayton.db.pg.PgTypeMapper :: toOrc dt = numeric, dtl = 4, dts = 0, isNul = true
            // Exception in thread "main" java.lang.IllegalArgumentException: precision 4 is out of range 1 .. 10
            //	at org.apache.orc.TypeDescription.withPrecision(TypeDescription.java:231)
            //	at mayton.db.pg.PgTypeMapper.toOrc(PgTypeMapper.java:86)
            //	at mayton.db.Db2Orc.prepareTypeDescription(Db2Orc.java:168)
            //	at mayton.db.Db2Orc.process(Db2Orc.java:271)
            //	at mayton.db.Db2Orc.process(Db2Orc.java:295)
            //	at mayton.db.Db2Orc.main(Db2Orc.java:303)
            TypeDescription typeDesc = TypeDescription.createDecimal();
            if (dataTypeScale != null) {
                typeDesc = typeDesc.withScale(dataTypeScale);
            }
            if (dataTypeLength != null) {
                typeDesc = typeDesc.withPrecision(dataTypeLength);
            }
            return typeDesc;
        } else if (databaseType.equalsIgnoreCase(PgTypes.TEXT.name())) {
            return TypeDescription.createString();
        } else if (databaseType.equalsIgnoreCase(PgTypes.VARCHAR.name())) {
            // TODO: Investigate for maxLength limitations
            TypeDescription typeDesc = TypeDescription.createVarchar();
            if (dataTypeLength != null) {
                typeDesc = typeDesc.withMaxLength(dataTypeLength);
            }
            return typeDesc;
        } else if (databaseType.equalsIgnoreCase(PgTypes.FLOAT8.name())) {
            return TypeDescription.createDouble();
        } else if (databaseType.equalsIgnoreCase(PgTypes.REAL.name()) || databaseType.equalsIgnoreCase(PgTypes.FLOAT4.name())) {
            // TODO: Test range
            return TypeDescription.createFloat();
        } else if (databaseType.equalsIgnoreCase(PgTypes.INT4.name()) || databaseType.equalsIgnoreCase(PgTypes.SERIAL.name())) {
            return TypeDescription.createInt();
        } else if (databaseType.equalsIgnoreCase(PgTypes.INT2.name())) {
            return TypeDescription.createShort();
        } else if (databaseType.equalsIgnoreCase(PgTypes.BPCHAR.name())) {
            return TypeDescription.createString();
        } else if (databaseType.equalsIgnoreCase(PgTypes.BIGSERIAL.name()) ||
                   databaseType.equalsIgnoreCase(PgTypes.INT8.name()) ||
                   databaseType.equalsIgnoreCase(PgTypes.SERIAL8.name())) {
            // TODO: Test range
            return TypeDescription.createLong();
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
            String columnTypeName  = rsmd.getColumnTypeName(i + 1);
            int isNullable         = rsmd.isNullable(i + 1);
            Object sqlFieldValue   = resultSet.getObject(i + 1);
            if (sqlFieldValue != null) {
                batch.cols[i].isNull[rowInBatch] = false;
                // TODO: PostgreSQL specific mapping
                if (columnTypeName.equals(PgTypes.BPCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.VARCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.TEXT.name().toLowerCase())) {
                    ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, ((String) sqlFieldValue).getBytes(UTF_8));
                } else if (columnTypeName.equals(PgTypes.INT4.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.SERIAL.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = ((int) sqlFieldValue);
                } else if (columnTypeName.equals(PgTypes.NUMERIC.name().toLowerCase())) {
                    BigDecimal bigDecimal = (BigDecimal) sqlFieldValue;
                    ColumnVector xColumnVector = batch.cols[i];
                    ((DecimalColumnVector) xColumnVector).vector[rowInBatch] = new HiveDecimalWritable(HiveDecimal.create(bigDecimal));
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
                batch.cols[i].isNull[rowInBatch] = true;
                // TODO: Stuped null replacement! Rework.
                if (columnTypeName.equals(PgTypes.BPCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.VARCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.TEXT.name().toLowerCase())) {
                    ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, EMPTY_BYTE_ARRAY);
                } else if (columnTypeName.equals(PgTypes.INT4.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = Integer.MIN_VALUE;
                } else if (columnTypeName.equals(PgTypes.NUMERIC.name().toLowerCase())) {
                    ((DecimalColumnVector) batch.cols[i]).vector[rowInBatch] = HIVE_DECIMAL_WRITABLE;
                } else if (columnTypeName.equals(PgTypes.FLOAT8.name().toLowerCase())) {
                    ((DoubleColumnVector) batch.cols[i]).vector[rowInBatch] = Double.NaN;
                }
            }
        }
    }
}
