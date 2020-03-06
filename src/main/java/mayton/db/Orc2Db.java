package mayton.db;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.*;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import static mayton.db.Utils.println;

public class Orc2Db extends GenericMainApplication {

    private static final boolean DEVMODE = true;

    static String logo = "\n" +
                    "8888888b.  888       .d8888b.   .d88888b.                  \n" +
                    "888  \"Y88b 888      d88P  Y88b d88P\" \"Y88b                 \n" +
                    "888    888 888             888 888     888                 \n" +
                    "888    888 88888b.       .d88P 888     888 888d888 .d8888b \n" +
                    "888    888 888 \"88b  .od888P\"  888     888 888P\"  d88P\"    \n" +
                    "888    888 888  888 d88P\"      888     888 888    888      \n" +
                    "888  .d88P 888 d88P 888\"       Y88b. .d88P 888    Y88b.    \n" +
                    "8888888P\"  88888P\"  888888888   \"Y88888P\"  888     \"Y8888P \n\n";

    static Logger logger = LogManager.getLogger(Orc2Db.class);

    @Override
    Options createOptions() {
        return new Options()
            .addOption("u", "url",       true, "JDBC url. (ex:jdbc:oracle:thin@localhost:1521/XE")
            .addOption("l", "login",     true, "JDBC login")
            .addOption("p", "password",  true, "JDBC password")
            .addOption("o", "orcfile",   true, "Orc file. (ex:big-data.orc)")
            .addOption("r", "rootTable", true, "Root table name")
            .addOption("m", "typeMapper",true, "Orc-to-database type mapper logic (default=mayton.db.pg.PGTypeMapper)"); // TODO: Depends on database type?
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

    public void go(String[] args) throws IOException, ParseException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException {
        String url = "";
        String login = "";
        String pwd = "";
        String orcFile = "";
        String rootTableName = "";
        Properties properties = new Properties();
        if (DEVMODE) {
            properties.load(new FileInputStream("sensitive.properties"));
        } else {
            if (args.length == 0) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(logo, createOptions() );
                return;
            } else {
                CommandLineParser parser = new DefaultParser();
                Options options = createOptions();
                CommandLine line = parser.parse(options, args);
                url = line.getOptionValue("u");
            }
        }
        process(properties);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("log4j1.compatibility", "true");
        System.setProperty("log4j.configuration", "log4j.properties");
        new Orc2Db().go(args);
    }

    public void process(Properties properties) throws IOException, ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {

        logger.info("Start");
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(
                new Path(String.valueOf(properties.getProperty("orcFile"))),
                OrcFile.readerOptions(conf));

        TypeDescription schema = reader.getSchema();

        logger.info("{}", generateCreationScript(schema, properties, properties.getProperty("rootTableName")));

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        int batchCount = 0;

        while (rows.nextBatch(batch)) {

            // Process batch
            logger.trace("# batch = {}, rows = {}, columCount = {}, partitionColumnCount = {}", batchCount++, batch.size,
                    batch.getDataColumnCount(),
                    batch.getPartitionColumnCount()
            );
            int batchSize = batch.size; // 1024 rows in batch

            logger.info("batchSize = {}", batchSize);

            // Transpose source data rows into 90 degrees destination tuples
            //ColumnVector[] columnVectors = new ColumnVector[batchSize];

            for (int i = 0; i < batch.cols.length; i++) {
                ColumnVector current = batch.cols[i];
                if (current instanceof LongColumnVector) {
                    logger.info("detected {} column as LongColumnVector", i);
                } else if (current instanceof BytesColumnVector) {
                    logger.info("detected {} column as BytesColumnVector", i);
                } else if (current instanceof DoubleColumnVector) {
                    logger.info("detected {} column as DoubleColumnVector", i);
                } else if (current instanceof ListColumnVector) {
                    logger.info("detected {} column as ListColumnVector", i);
                } else {
                    logger.warn("unable to detect {} column type {}", current.getClass());
                }
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

        logger.info("Processed batches : {}", batchCount);

        rows.close();

        reader.close();

        logger.info("Finish");
    }
}
