package mayton.db;

import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataMaskDescription;
import org.apache.orc.EncryptionKey;
import org.apache.orc.EncryptionVariant;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ReaderImpl;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class OrcSchemaDbTransformerTest {

    @Test(expected = IllegalArgumentException.class)
    public void test() {
        OrcSchemaDbTransformer orcSchemaDbTransformer = OrcSchemaDbTransformer.getInstance();
        orcSchemaDbTransformer.apply(null);
    }

    public void assume_orc_schema_db_transformer_return_non_null_collection() {
        Reader reader = new MockReader();
        OrcSchemaDbTransformer orcSchemaDbTransformer = OrcSchemaDbTransformer.getInstance();
        assertNotNull(orcSchemaDbTransformer.apply(reader));
    }

}
