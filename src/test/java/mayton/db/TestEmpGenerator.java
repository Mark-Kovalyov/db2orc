package mayton.db;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;


import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Random;


public class TestEmpGenerator {

    public static void main(String[] args) throws IOException {
        TypeDescription schema = createSchema();
        String pathString = "src/test/resources/test-emp.orc";
        Path pathObject = new Path(pathString);
        Configuration conf = new Configuration();
        FileSystem fs = new Path(".").getFileSystem(conf);
        fs.delete(pathObject, false);
        int batchSize = 50000;
        try (Writer writer = WriterHelper.createWriter(fs, pathString, schema)) {
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

    static Random      random = new Random();
    static MathContext mathContext = new MathContext(2, RoundingMode.HALF_DOWN);

    private static void appendRow(VectorizedRowBatch batch, int row, int v) {
        int i = 0;

        ((LongColumnVector)      batch.cols[i++]).vector[row] = v;
        ((BytesColumnVector)     batch.cols[i++]).setVal(row, "King".getBytes(StandardCharsets.UTF_8));
        ((BytesColumnVector)     batch.cols[i++]).setVal(row, "director".getBytes(StandardCharsets.UTF_8));
        ((LongColumnVector)      batch.cols[i++]).vector[row] = random.nextInt(20);

        ((TimestampColumnVector) batch.cols[i++]).set(row, new Timestamp(System.currentTimeMillis()));
        double sal = 3000.0 + 1000.0 * random.nextGaussian();
        ((DecimalColumnVector)   batch.cols[i++]).vector[row] = new HiveDecimalWritable(new BigDecimal(sal).round(mathContext).toPlainString());
        double comm = 1000.0 + 500.0 * random.nextGaussian();
        ((DecimalColumnVector)   batch.cols[i++]).vector[row] = new HiveDecimalWritable(new BigDecimal(comm).toPlainString());
        ((LongColumnVector)      batch.cols[i++]).vector[row] = random.nextInt(3);
    }

    private static TypeDescription createSchema() {
        // struct<empno:bigint,ename:varchar(30),job:varchar(30),mgr:bigint,hiredate:date,sal:decimal(38,10),comm:decimal(38,10),depno:bigint>
        TypeDescription td = TypeDescription.createStruct();
        td.addField("empno",    TypeDescription.createLong());
        td.addField("ename",    TypeDescription.createVarchar().withMaxLength(30));
        td.addField("job",      TypeDescription.createVarchar().withMaxLength(30));
        td.addField("mgr",      TypeDescription.createLong());
        td.addField("hiredate", TypeDescription.createTimestamp());
        td.addField("sal",      TypeDescription.createDecimal()); // decimal(38,10)
        td.addField("comm",     TypeDescription.createDecimal());
        td.addField("depno",    TypeDescription.createLong());
        return td;
    }
}
