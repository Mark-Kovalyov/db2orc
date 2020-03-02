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

public class OrcSchemaDbTransformerTest {

    @Test(expected = IllegalArgumentException.class)
    public void test() {
        OrcSchemaDbTransformer orcSchemaDbTransformer = OrcSchemaDbTransformer.getInstance();
        orcSchemaDbTransformer.apply(null);
    }

    public void test2() {
        Reader reader = new Reader() {
            @Override
            public long getNumberOfRows() {
                return 0;
            }

            @Override
            public long getRawDataSize() {
                return 0;
            }

            @Override
            public long getRawDataSizeOfColumns(List<String> list) {
                return 0;
            }

            @Override
            public long getRawDataSizeFromColIndices(List<Integer> list) {
                return 0;
            }

            @Override
            public List<String> getMetadataKeys() {
                return null;
            }

            @Override
            public ByteBuffer getMetadataValue(String s) {
                return null;
            }

            @Override
            public boolean hasMetadataValue(String s) {
                return false;
            }

            @Override
            public CompressionKind getCompressionKind() {
                return null;
            }

            @Override
            public int getCompressionSize() {
                return 0;
            }

            @Override
            public int getRowIndexStride() {
                return 0;
            }

            @Override
            public List<StripeInformation> getStripes() {
                return null;
            }

            @Override
            public long getContentLength() {
                return 0;
            }

            @Override
            public ColumnStatistics[] getStatistics() {
                return new ColumnStatistics[0];
            }

            @Override
            public TypeDescription getSchema() {
                return null;
            }

            @Override
            public List<OrcProto.Type> getTypes() {
                return null;
            }

            @Override
            public OrcFile.Version getFileVersion() {
                return null;
            }

            @Override
            public OrcFile.WriterVersion getWriterVersion() {
                return null;
            }

            @Override
            public OrcProto.FileTail getFileTail() {
                return null;
            }

            @Override
            public EncryptionKey[] getColumnEncryptionKeys() {
                return new EncryptionKey[0];
            }

            @Override
            public DataMaskDescription[] getDataMasks() {
                return new DataMaskDescription[0];
            }

            @Override
            public EncryptionVariant[] getEncryptionVariants() {
                return new EncryptionVariant[0];
            }

            @Override
            public List<StripeStatistics> getVariantStripeStatistics(EncryptionVariant encryptionVariant) throws IOException {
                return null;
            }

            @Override
            public Options options() {
                return null;
            }

            @Override
            public RecordReader rows() throws IOException {
                return null;
            }

            @Override
            public RecordReader rows(Options options) throws IOException {
                return null;
            }

            @Override
            public List<Integer> getVersionList() {
                return null;
            }

            @Override
            public int getMetadataSize() {
                return 0;
            }

            @Override
            public List<OrcProto.StripeStatistics> getOrcProtoStripeStatistics() {
                return null;
            }

            @Override
            public List<StripeStatistics> getStripeStatistics() throws IOException {
                return null;
            }

            @Override
            public List<StripeStatistics> getStripeStatistics(boolean[] booleans) throws IOException {
                return null;
            }

            @Override
            public List<OrcProto.ColumnStatistics> getOrcProtoFileStatistics() {
                return null;
            }

            @Override
            public ByteBuffer getSerializedFileFooter() {
                return null;
            }

            @Override
            public void close() throws IOException {

            }
        };
        OrcSchemaDbTransformer orcSchemaDbTransformer = OrcSchemaDbTransformer.getInstance();
        orcSchemaDbTransformer.apply(reader);
    }

}
