package mayton.db;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;


import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Random;


public class TestEmpDeptGenerator {

    public static void main(String[] args) throws IOException {
        TypeDescription schema = createSchema();
        String pathString = "src/test/resources/test-emp-dept.orc";
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
                appendDeptRow(batch, batch.size++, b);
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

    private static void enrichEmpRows(@NotNull ListColumnVector empVector, VectorizedRowBatch batch) {
        int j = 0;
        StructColumnVector structColumnVector = new StructColumnVector();

        for (int k = 0; k < 10; k++) {

        }
    }

    private static void appendDeptRow(@NotNull VectorizedRowBatch batch, int row, int v) {
        int i = 0;
        ((LongColumnVector)    batch.cols[i++]).vector[row] = v; // deptno
        ((BytesColumnVector)   batch.cols[i++]).setVal(row, "Dname".getBytes(StandardCharsets.UTF_8)); // dname
        ((BytesColumnVector)   batch.cols[i++]).setVal(row, "Location".getBytes(StandardCharsets.UTF_8));   // loc
        ListColumnVector listColumnVector = new ListColumnVector();
        //enrichEmpRows(listColumnVector, batch);
        ((ListColumnVector)    batch.cols[i++]).child = listColumnVector;
    }

    private static TypeDescription createSchema() {
        TypeDescription td = TypeDescription.fromString(
                "struct<deptno:bigint," +
                        "dname:varchar(14)," +
                        "loc:varchar(13)," +
                        "emp:array<" +
                            "struct<empno:bigint," +
                                   "ename:varchar(30)," +
                                   "job:varchar(30)," +
                                   "mgr:bigint," +
                                   "hiredate:timestamp," +
                                   "sal:decimal(38,10)," +
                                   "comm:decimal(38,10)," +
                                   "depno:bigint>>>");

        return td;
    }
}
