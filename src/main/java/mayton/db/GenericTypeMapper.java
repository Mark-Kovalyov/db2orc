package mayton.db;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Range;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class GenericTypeMapper implements ITypeMapper {

    @Override
    public @NotNull String fromOrc(@NotNull TypeDescription typeDescription) {
        return "VARCHAR";
    }

    @Override
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, Optional<Integer> databaseLength, Optional<Integer> databasePrecision, boolean isNullable) {
        return TypeDescription.createString();
    }

    @Override
    public void toOrcVectorized(@NotNull VectorizedRowBatch batch, @Range(from = 0, to = Integer.MAX_VALUE) int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        // Nothing to do
    }


}