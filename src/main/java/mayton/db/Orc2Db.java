package mayton.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.*;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.util.Properties;

public class Orc2Db {

    static Logger logger = LogManager.getLogger(Orc2Db.class);

    public static String generateCreationScript(@NotNull TypeDescription schema) {
        StringBuilder stringBuilder = new StringBuilder("create table(");
        for(TypeDescription typeDescription : schema.getChildren()) {
            logger.info("{} scale = {}, category = {}, precision = {}", typeDescription, typeDescription.getScale(), typeDescription.getCategory(), typeDescription.getPrecision());
        }
        for(String name : schema.getFieldNames()) {
            logger.info("name = {}", name);
        }

        return stringBuilder.toString();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("log4j1.compatibility", "true");
        System.setProperty("log4j.configuration", "log4j.properties");
        Properties properties = new Properties();
        properties.load(new FileInputStream("sensitive.properties"));
        logger.info("Start");
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        //conf.set("fs.s3a.endpoint", "s3://myS3");

        Reader reader = OrcFile.createReader(
                new Path(properties.getProperty("file")),
                OrcFile.readerOptions(conf));

        TypeDescription schema = reader.getSchema();

        generateCreationScript(schema);

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        int batchCount = 0;
        while (rows.nextBatch(batch)) {
            logger.info("# batch = {}, rows = {}, columCount = {}, partitionColumnCount = {}", batchCount++, batch.size,
                    batch.getDataColumnCount(),
                    batch.getPartitionColumnCount()
            );

            for (int r=0; r < batch.size; r++) {
                logger.trace("# row {}", r);
            }
        }
        rows.close();

        reader.close();

        logger.info("Finish");
    }
}
