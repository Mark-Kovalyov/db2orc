package mayton.db;

import org.apache.orc.Reader;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public class OrcSchemaDbTransformer implements Function<Reader, Set<TableEntity>> {

    private static OrcSchemaDbTransformer instance = null;

    public OrcSchemaDbTransformer() {
    }

    public static OrcSchemaDbTransformer getInstance() {
        if (instance == null) {
            instance = new OrcSchemaDbTransformer();
        }
        return instance;
    }


    @Override
    public Set<TableEntity> apply(Reader reader) {
        checkArgument(reader != null);
        Set<TableEntity> tableEntities = new LinkedHashSet<>();
        // Enumerate content

        return tableEntities;
    }
}
