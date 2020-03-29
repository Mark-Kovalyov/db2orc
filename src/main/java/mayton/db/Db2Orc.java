package mayton.db;

import mayton.db.pg.PGTypeMapper;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;

public class Db2Orc extends GenericMainApplication {

    public static Logger logger = LogManager.getLogger(Db2Orc.class);

    private static final boolean DEVMODE = false;

    @Override
    String createLogo() {
        return "\n     888 888       .d8888b.                           \n" +
                "     888 888      d88P  Y88b                          \n" +
                "     888 888             888                          \n" +
                " .d88888 88888b.       .d88P  .d88b.  888d888 .d8888b \n" +
                "d88\" 888 888 \"88b  .od888P\"  d88\"\"88b 888P\"  d88P\"    \n" +
                "888  888 888  888 d88P\"      888  888 888    888      \n" +
                "Y88b 888 888 d88P 888\"       Y88..88P 888    Y88b.    \n" +
                " \"Y88888 88888P\"  888888888   \"Y88P\"  888     \"Y8888P\n\n";
    }

    @Override
    Options createOptions() {
        return new Options()
                .addRequiredOption("u", "url",       true, "JDBC url. (ex:jdbc:oracle:thin@localhost:1521/XE")
                .addRequiredOption("l", "login",     true, "JDBC login")
                .addRequiredOption("p", "password",  true, "JDBC password")
                .addRequiredOption("o", "orcfile",   true, "Orc file. (ex:big-data.orc)")
                .addOption("s", "selectexpr",true, "SELECT-expression (ex: SELECT * FROM EMP)")
                .addOption("t", "tablename", true, "Table or View name")
                .addOption("co", "orc.compression",  true, "Orc file compression := { NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD }")
                .addOption("bf", "orc.bloomColumns", true, "Orc file bloom filter columns (comma-separated)");
    }

    private static void appendRow(@NotNull VectorizedRowBatch batch, int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            Object sqlFieldValue = resultSet.getObject(i + 1);
            Class<? extends Object> sqlType = sqlFieldValue.getClass();
            logger.trace("column = {}, sqlType = {}, value = '{}'", i, sqlType, sqlFieldValue);
            if (sqlType == String.class) {
                ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, ((String) sqlFieldValue).getBytes(StandardCharsets.UTF_8));
            } else if (sqlType == java.sql.Timestamp.class) {
                ((TimestampColumnVector) batch.cols[i]).set(rowInBatch, (Timestamp) sqlFieldValue);
            } else if (sqlType == java.math.BigDecimal.class) {
                ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = ((BigDecimal) sqlFieldValue).longValue();
            // TODO: Get rid of Postgre-specific types
            } else if (sqlType == org.postgresql.util.PGobject.class) {
                ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, ("bson-value-is-here").getBytes(StandardCharsets.UTF_8));
            } else {
                throw new RuntimeException("Unable to append row for sqlType = " + sqlType);
            }
        }
    }

    public void processWithWriter(@NotNull Writer writer, @NotNull TypeDescription schema, @NotNull Connection connection, @NotNull String query, int batchSize) throws IOException, SQLException {
        // TODO: Hardcode
        VectorizedRowBatch batch = schema.createRowBatch(batchSize);
        Statement statement = connection.createStatement();
        logger.debug("execute query : {}", query);
        statement.executeQuery(query);
        ResultSet resultSet = statement.getResultSet();
        long rows = 0;
        long batches = 0;
        while(resultSet.next()) {
            rows++;
            appendRow(batch, batch.size, resultSet);
            batch.size++;
            if (batch.size >= batchSize) {
                writer.addRowBatch(batch);
                batches++;
                batch.reset();
            }
        }
        if (batch.size >= batchSize) {
            writer.addRowBatch(batch);
            batches++;
            batch.reset();
        }

        resultSet.close();
        statement.close();

        logger.info("Successfully write {} rows and {} batches", rows, batches);
    }

    public void process(@NotNull Properties properties) throws SQLException, IOException {
        logger.info("[1] Start process");

        Connection connection = DriverManager.getConnection(
                properties.getProperty("url"),
                properties.getProperty("login"),
                properties.getProperty("password"));

        logger.info("[2] Read metadata from DB");

        DatabaseMetaData metadata = connection.getMetaData();

        String tableName = (String) properties.getOrDefault("tablename", null);
        String selectExpr = (String) properties.getOrDefault("selectexpr", null);

        ResultSet res = SQLUtils.getColumns(metadata, tableName);

        TypeDescription schema = TypeDescription.createStruct();

        logger.info("[3] Process type mapper");

        TypeMapper typeMapper = new PGTypeMapper();

        while(res.next()) {
            String columnName = res.getString("COLUMN_NAME");
            int dataType      = res.getInt("DATA_TYPE");
            String typeName   = res.getString("TYPE_NAME");
            int nullAllowed   = res.getInt("NULLABLE");
            logger.info("{} , dataType = {}, typeName = {}, nullAllowred = {}", columnName, dataType, typeName, nullAllowed);
            // TODO: Pass length, precission, nullable
            TypeDescription typeDescription = typeMapper.toOrc(typeName, 30, 0, true);
            logger.info("typeDescription = {}", typeDescription);
            schema.addField(columnName, typeDescription);
        }

        res.close();

        String orcFilePath = properties.getProperty("orcfile");
        logger.info("[4] Export ORC file = {}", orcFilePath);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        String userDir = System.getProperty("user.dir");
        logger.info("[4.3] User dir = {}", userDir);
        org.apache.hadoop.fs.Path currentDirPath = new org.apache.hadoop.fs.Path(userDir);
        logger.info("[5] currentDirPath = {}", currentDirPath.toString());
        org.apache.hadoop.fs.FileSystem currentDirPathFileSystem = currentDirPath.getFileSystem(conf);
        logger.info("[6] fs.canonicalServName = {}", currentDirPathFileSystem.getCanonicalServiceName());
        currentDirPathFileSystem.delete(new Path(orcFilePath), false);
        logger.info("[6.1] create Orc-Writer with schema");
        Writer writer = OrcUtils.createWriter(currentDirPathFileSystem, orcFilePath, schema, properties);
        if (selectExpr != null) {
            // TODO
            throw new RuntimeException("Non implemented yet!");
        } else if (tableName != null) {
            processWithWriter(writer, schema, connection, "SELECT * FROM " + tableName, 50_000);
        } else {
            throw new IllegalArgumentException("Undefined table or SQL-expression for export!");
        }
        writer.close();
        logger.info("[6.2] Orc-Writer closed");
        connection.close();
        logger.info("[7] Finish!");
    }

    public void process(String[] args) throws SQLException, ParseException, IOException, ClassNotFoundException {

        Properties properties = new Properties();
        if (DEVMODE) {
            properties.load(new FileInputStream("sensitive.properties"));
        } else {
            CommandLineParser parser = new DefaultParser();
            Options options = createOptions();
            if (args.length == 0) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(createLogo(), createOptions());
                return;
            } else {
                CommandLine line = parser.parse(options, args);
                // Mandatory
                properties.put("url",      line.getOptionValue("u"));
                properties.put("login",    line.getOptionValue("l"));
                properties.put("password", line.getOptionValue("p"));
                properties.put("orcfile", line.getOptionValue("o"));
                // Optional
                if (line.hasOption("t"))  properties.put("tablename", line.getOptionValue("t"));
                if (line.hasOption("s"))  properties.put("selectexpr", line.getOptionValue("s"));
                if (line.hasOption("co")) properties.put("orc.compression", line.getOptionValue("co"));
                if (line.hasOption("bf")) properties.put("orc.bloomColumns", line.getOptionValue("bf"));
            }
        }
        process(properties);
    }

    public static void main(String[] args) throws SQLException, ParseException, IOException, ClassNotFoundException {
        System.setProperty("log4j1.compatibility", "true");
        System.setProperty("log4j.configuration", "log4j.properties");
        new Db2Orc().process(args);
    }


}
