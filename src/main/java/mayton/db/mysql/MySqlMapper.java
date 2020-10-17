package mayton.db.mysql;

import mayton.db.GenericTypeMapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class MySqlMapper extends GenericTypeMapper {

    @Override
    public @NotNull String fromOrc(@NotNull TypeDescription typeDescription) {
        return super.fromOrc(typeDescription);
    }

    @Override
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, Optional<Integer> dataTypeLength, Optional<Integer> dataTypeScale, boolean isNullable) {
        return super.toOrc(databaseType, dataTypeLength, dataTypeScale, isNullable);
    }

    @Override
    public void toOrcVectorized(@NotNull VectorizedRowBatch batch, int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        super.toOrcVectorized(batch, rowInBatch, resultSet);
    }
}
