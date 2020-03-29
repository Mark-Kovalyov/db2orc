package mayton.db;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ITypeMapper {

    @NotNull @Contract(pure = true) String fromOrc(@NotNull TypeDescription typeDescription);

    @NotNull @Contract(pure = true) TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer databaseLength, @Nullable Integer databasePrecision, boolean isNullable);

    @Contract(pure = false) void toOrcVectorized(@NotNull VectorizedRowBatch batch, int rowInBatch, @NotNull ResultSet resultSet) throws SQLException;

}
