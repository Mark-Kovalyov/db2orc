package mayton.db;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;


import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;


public class TestGenerator {

    public static void main(String[] args) throws IOException {
        TypeDescription schema = createSchema();
        Configuration conf = new Configuration();
        FileSystem fs = new Path(".").getFileSystem(conf);
        Path testFilePath = new Path("src/test/sample.orc");
        fs.delete(testFilePath, false);
        int batchSize = 50000;
        try (Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(fs.getConf())
                .setSchema(schema)
                .compress(CompressionKind.ZLIB)
                .stripeSize(128 * 1024 * 1024)
                .bufferSize(256 * 1024)
                .rowIndexStride(10000)
                .version(OrcFile.Version.V_0_12))) {
            VectorizedRowBatch batch = schema.createRowBatch(batchSize);
            int numRows = 200;
            int tail = 0;
            for (int b = 0; b < numRows; tail = b++ % batchSize) {
                appendRow(batch, batch.size++, b);
                if (tail == 0) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (tail != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
    }

    private static void appendRow(VectorizedRowBatch batch, int row, int v) {
        int i = 0;
        ((LongColumnVector)   batch.cols[i++]).vector[row] = v;
        ((BytesColumnVector)  batch.cols[i++]).setVal(row, "King".getBytes(StandardCharsets.UTF_8));
        ((BytesColumnVector)  batch.cols[i++]).setVal(row, "director".getBytes(StandardCharsets.UTF_8));
        ((LongColumnVector)   batch.cols[i++]).vector[row] = v * 2;
        // TODO: Fix for incorrect date "hiredate":"\u0000843-04-08"
        ((LongColumnVector)   batch.cols[i++]).vector[row] = System.currentTimeMillis();
        ((DecimalColumnVector) batch.cols[i++]).vector[row] = new HiveDecimalWritable("17.1");
        ((DecimalColumnVector) batch.cols[i++]).vector[row] = new HiveDecimalWritable("31.3");
        ((LongColumnVector)  batch.cols[i++]).vector[row] = v * 3;
    }

    private static TypeDescription createSchema() {
        // struct<empno:bigint,ename:varchar(30),job:varchar(30),mgr:bigint,hiredate:date,sal:decimal(38,10),comm:decimal(38,10),depno:bigint>
        TypeDescription td = TypeDescription.createStruct();
        td.addField("empno",    TypeDescription.createLong());
        td.addField("ename",    TypeDescription.createVarchar().withMaxLength(30));
        td.addField("job",      TypeDescription.createVarchar().withMaxLength(30));
        td.addField("mgr",      TypeDescription.createLong());
        td.addField("hiredate", TypeDescription.createDate());
        td.addField("sal",      TypeDescription.createDecimal()); // decimal(38,10)
        td.addField("comm",     TypeDescription.createDecimal());
        td.addField("depno",    TypeDescription.createLong());
        return td;
    }
}
