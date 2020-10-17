package mayton.db.oracle;

import mayton.db.GenericTypeMapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import javax.annotation.concurrent.ThreadSafe;
import java.sql.ResultSet;
import java.sql.SQLException;

@ThreadSafe
public class OracleTypeMapper extends GenericTypeMapper {

    @Override
    public @NotNull String fromOrc(@NotNull TypeDescription typeDescription) {
        // TODO:
        return "VARCHAR2(4000)";
    }

    @Override
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer dataTypeLength, @Nullable Integer dataTypeScale, boolean isNullable) {
        // TODO:
        return TypeDescription.createString();
    }

    @Override
    public void toOrcVectorized(@NotNull VectorizedRowBatch batch, @Range(from = 0, to = Integer.MAX_VALUE) int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        // TODO:
    }

}
