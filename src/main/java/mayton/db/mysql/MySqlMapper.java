package mayton.db.mysql;

import mayton.db.GenericTypeMapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlMapper extends GenericTypeMapper {

    @Override
    public @NotNull String fromOrc(@NotNull TypeDescription typeDescription) {
        return super.fromOrc(typeDescription);
    }

    @Override
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer databaseLength, @Nullable Integer databasePrecision, boolean isNullable) {
        return super.toOrc(databaseType, databaseLength, databasePrecision, isNullable);
    }

    @Override
    public void toOrcVectorized(@NotNull VectorizedRowBatch batch, int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        super.toOrcVectorized(batch, rowInBatch, resultSet);
    }
}
