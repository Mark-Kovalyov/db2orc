package mayton.db;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GenericTypeMapper implements ITypeMapper {

    @NotNull
    public String fromOrc(@NotNull TypeDescription typeDescription) {
        return "VARCHAR";
    }

    @NotNull
    public TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer databaseLength, @Nullable Integer databasePrecision, boolean isNullable) {
        return TypeDescription.createString();
    }

    @Override
    public void toOrcVectorized(@NotNull VectorizedRowBatch batch, int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        // TODO: Implement
    }

}