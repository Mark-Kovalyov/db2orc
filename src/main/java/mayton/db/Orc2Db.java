package mayton.db;

import mayton.db.h2.H2TypeMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.*;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Orc2Db {

    static Logger logger = LogManager.getLogger(Orc2Db.class);

    static TypeMapper typeMapper = new H2TypeMapper();

    public List<Pair<String, String>> generate(TypeDescription schema) {
        List<Pair<String, String>> list = new ArrayList<>();

        return list;
    }

    public static String generateCreationScript(@NotNull TypeDescription schema) {
        StringBuilder sql = new StringBuilder("create table(\n");
        List<TypeDescription> td = schema.getChildren();
        List<String> name = schema.getFieldNames();
        for(int i = 0;i<td.size();i++) {
            sql.append(name.get(i));
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
        Properties properties = new Properties();
        properties.load(new FileInputStream("sensitive.properties"));
        logger.info("Start");
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        //conf.set("fs.s3a.endpoint", "s3://myS3");

        Reader reader = OrcFile.createReader(
                new Path(properties.getProperty("file")),
                OrcFile.readerOptions(conf));

        TypeDescription schema = reader.getSchema();

        logger.info("{}",generateCreationScript(schema));

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        int batchCount = 0;
        while (rows.nextBatch(batch)) {
            logger.trace("# batch = {}, rows = {}, columCount = {}, partitionColumnCount = {}", batchCount++, batch.size,
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
