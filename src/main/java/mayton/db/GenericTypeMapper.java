package mayton.db;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * Orc DataTypes
 *
 *     Integer
 *         boolean (1 bit)
 *         tinyint (8 bit)
 *         smallint (16 bit)
 *         int (32 bit)
 *         bigint (64 bit)
 *
 *     Floating point
 *         float
 *         double
 *
 *     String types
 *         string
 *         char
 *         varchar
 *
 *     Binary blobs
 *         binary
 *
 *     Date/time
 *         timestamp
 *         timestamp with local time zone
 *         date
 *
 *     Compound types
 *         struct
 *         list
 *         map
 *         union
 */
public class GenericTypeMapper implements ITypeMapper {

    @Override
    public @NotNull String fromOrc(@NotNull TypeDescription typeDescription) {
        return "VARCHAR";
    }

    @Override
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer dataTypeLength, @Nullable Integer dataTypeScale, boolean isNullable) {
        return TypeDescription.createString();
    }

    @Override
    public void toOrcVectorized(@NotNull VectorizedRowBatch batch, @Range(from = 0, to = Integer.MAX_VALUE) int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        // Nothing to do
    }


}