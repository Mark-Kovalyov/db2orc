package mayton.db;

import mayton.db.h2.H2TypeMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.*;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Orc2Db {

    static Logger logger = LogManager.getLogger(Orc2Db.class);



    public List<Pair<String, String>> generate(TypeDescription schema) {
        List<Pair<String, String>> list = new ArrayList<>();

        return list;
    }

    public static String generateCreationScript(@NotNull TypeDescription schema, @NotNull Properties properties, @NotNull String tableName) throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class typeMapperClass = Class.forName(properties.getProperty("mapper"));
        TypeMapper typeMapper = (TypeMapper) typeMapperClass.getDeclaredConstructors()[0].newInstance(null);
        StringBuilder sql = new StringBuilder("\ncreate table ");
        sql.append(tableName);
        sql.append(" (\n");
        List<TypeDescription> td = schema.getChildren();
        List<String> name = schema.getFieldNames();
        for (int i = 0; i < td.size(); i++) {
            sql.append(StringUtils.replace(name.get(i).toLowerCase(), " ", "_"));
            sql.append(" ");
            String orcType = td.get(i).getCategory().getName();
            sql.append(typeMapper.fromOrc(td.get(i)));
            if (i != td.size() - 1) {
                sql.append(",\n");
            } else {
                sql.append("\n");
            }

        }
        sql.append(");\n\n");
        return sql.toString();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("log4j1.compatibility", "true");
        System.setProperty("log4j.configuration", "log4j.properties");
        // System.setProperty("illegal-access","warn");

        Properties properties = new Properties();
        properties.load(new FileInputStream("sensitive.properties"));
        logger.info("Start");
        org.apache.hadoop.conf.Configuration conf = new Configuration();

        Reader reader = OrcFile.createReader(
                new Path(String.valueOf(properties.getOrDefault("db2orc.inFile", "orc/sample-01.orc"))),
                OrcFile.readerOptions(conf));

        TypeDescription schema = reader.getSchema();

        logger.info("{}", generateCreationScript(schema, properties, "tab00005"));

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        int batchCount = 0;

        while (rows.nextBatch(batch)) {
            logger.trace("# batch = {}, rows = {}, columCount = {}, partitionColumnCount = {}", batchCount++, batch.size,
                    batch.getDataColumnCount(),
                    batch.getPartitionColumnCount()
            );
            int batchSize = batch.size;
            // Transpose source data rows into 90 degrees destination tuples
            for (int i = 0; i < batchSize; i++) {
                ColumnVector currentColumnVector = batch.cols[i];
            }

            //BytesColumnVector  ownershipIdByteVector   = (BytesColumnVector)  batch.cols[0];
            //LongColumnVector    transactionIdLongVector   = (LongColumnVector)   batch.cols[1];
            //BytesColumnVector   byteVector2  = (BytesColumnVector)  batch.cols[2];
            //LongColumnVector    longVector2  = (LongColumnVector)   batch.cols[3];

            for (int r = 0; r < batch.size; r++) {

                //byte[] ownershipIdByte  = ownershipIdByteVector.vector[r];
                //long   transactionIdLong = transactionIdLongVector.vector[r];

                //double floatValue = (float) floatVector.vector[r];
                //boolean boolValue = booleanVector.vector[r] != 0;
                //String stringValue = new String(stringVector.vector[r], stringVector.start[r], stringVector.length[r]);

                /*logger.info("# row {}, transactionIdLong = {}", r, transactionIdLong);

                if (ownershipIdByte.length > 0) {
                    logger.info("# row {}, ownershipIdByteVector = {} .length = {}", r, new String(ownershipIdByte), ownershipIdByte.length);
                }*/
            }
        }
        rows.close();

        reader.close();

        logger.info("Finish");
    }
}
