package mayton.db;

import mayton.db.pg.PgTypeMapper;
import mayton.db.pg.PgTypes;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Optional;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("java:S2629")
public class Db2Orc extends GenericMainApplication {

    public static Logger logger = LogManager.getLogger(Db2Orc.class);

    private static final boolean DEVMODE = false;

    @NotNull
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

    @NotNull
    @Override
    Options createOptions() {
        return new Options()
                .addRequiredOption("u", "url", true, "JDBC url. (ex:jdbc:oracle:thin@localhost:1521/XE")
                .addRequiredOption("l", "login", true, "JDBC login")
                .addRequiredOption("p", "password", true, "JDBC password")
                .addRequiredOption("o", "orcfile", true, "Orc file. (ex:big-data.orc)")
                .addOption("s", "selectexpr", true, "SELECT-expression (ex: SELECT * FROM EMP)")
                .addOption("t", "tablename", true, "Table or View name")
                .addOption("co", "orc.compression", true, "Orc file compression := { NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD }")
                .addOption("bc", "orc.bloomcolumns", true, "Orc file bloom filter columns (comma-separated)")
                .addOption("bf", "orc.bloomfilterfpp", true, "False positive probability for bloom filter [0.75..0.99]")
                .addOption("ss", "orc.stripesize", true, "The writer stores the contents of the" +
                                   " stripe in memory until this memory limit is reached and the stripe" +
                                   " is flushed to the HDFS file and the next stripe started")

                .addOption("ri", "orc.rowindexstride", true, "Row index stride [0..1000], 0 - means no index will be.");
    }

    private static byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private static void appendRow(@NotNull VectorizedRowBatch batch, int rowInBatch, @NotNull ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            String columnClassName = rsmd.getColumnClassName(i + 1);
            String columnTypeName = rsmd.getColumnTypeName(i + 1);
            Object sqlFieldValue = resultSet.getObject(i + 1);
            if (sqlFieldValue != null) {
                // TODO: Generic JDBC-Type-Based mapping
                /*Class<? extends Object> jdbcSqlType = sqlFieldValue.getClass();
                logger.trace("column = {}, sqlType = {}, value = '{}'", i, jdbcSqlType, sqlFieldValue);
                if (jdbcSqlType == String.class) {
                    ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, ((String) sqlFieldValue).getBytes(UTF_8));
                } else if (jdbcSqlType == java.sql.Timestamp.class) {
                    ((TimestampColumnVector) batch.cols[i]).set(rowInBatch, (Timestamp) sqlFieldValue);
                } else if (jdbcSqlType == java.math.BigDecimal.class) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = ((BigDecimal) sqlFieldValue).longValue();
                    // TODO: Get rid of Postgre-specific types
                } else if (jdbcSqlType == Integer.class) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = (long) ((int) sqlFieldValue);
                } else {
                    throw new RuntimeException("Unable to append row for sqlType = " + jdbcSqlType +
                            " columnClassName = " + columnClassName + " columnTypeName = " + columnTypeName +
                            " during vectorized row batch generation, value = " + sqlFieldValue);
                }*/

                // TODO: PostgreSQL specific mapping
                if (columnTypeName.equals(PgTypes.BPCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.VARCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.TEXT.name().toLowerCase())) {
                    ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, ((String) sqlFieldValue).getBytes(UTF_8));
                } else if (columnTypeName.equals(PgTypes.INT4.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.SERIAL.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = ((int) sqlFieldValue);
                } else if (columnTypeName.equals(PgTypes.NUMERIC.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = ((BigDecimal) sqlFieldValue).longValue();
                } else if (columnTypeName.equals(PgTypes.FLOAT8.name().toLowerCase())) {
                    ((DoubleColumnVector) batch.cols[i]).vector[rowInBatch] = (double) sqlFieldValue;
                } else if (columnTypeName.equals(PgTypes.TIMESTAMPTZ.name().toLowerCase())) {
                    ((TimestampColumnVector) batch.cols[i]).set(rowInBatch, (Timestamp) sqlFieldValue);
                } else if (columnTypeName.equals(PgTypes.JSONB.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.POINT.name().toLowerCase())) {
                    // TODO: Stuped JSONB/POINT stub
                    ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, "{}".getBytes(UTF_8));
                } else {
                    throw new RuntimeException("Unable to append row for columnClassName = " +
                            columnClassName + " columnTypeName = " + columnTypeName +
                            " during vectorized row batch generation, value = " + sqlFieldValue);
                }
            } else {
                // TODO: Stuped null replacement! Rework.
                if (columnTypeName.equals(PgTypes.BPCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.VARCHAR.name().toLowerCase()) ||
                        columnTypeName.equals(PgTypes.TEXT.name().toLowerCase())) {
                    ((BytesColumnVector) batch.cols[i]).setVal(rowInBatch, EMPTY_BYTE_ARRAY);
                } else if (columnTypeName.equals(PgTypes.INT4.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = Integer.MIN_VALUE;
                } else if (columnTypeName.equals(PgTypes.NUMERIC.name().toLowerCase())) {
                    ((LongColumnVector) batch.cols[i]).vector[rowInBatch] = Long.MIN_VALUE;
                } else if (columnTypeName.equals(PgTypes.FLOAT8.name().toLowerCase())) {
                    ((DoubleColumnVector) batch.cols[i]).vector[rowInBatch] = Double.NaN;
                }
            }
        }
    }

    public void processWithWriter(@NotNull Writer writer, @NotNull TypeDescription schema, @NotNull Connection connection, @NotNull String query, int batchSize) throws IOException, SQLException {
        // TODO: Hardcode
        VectorizedRowBatch batch = schema.createRowBatch(batchSize);
        try (Statement statement = connection.createStatement()) {
            logger.debug("execute query : {}", query);
            statement.executeQuery(query);
            ResultSet resultSet = statement.getResultSet();
            long rows = 0;
            long batches = 0;
            while (resultSet.next()) {
                rows++;
                appendRow(batch, batch.size, resultSet);
                batch.size++;
                if (batch.size >= batchSize) {
                    writer.addRowBatch(batch);
                    batches++;
                    batch.reset();
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
                batches++;
                batch.reset();
            }
            resultSet.close();
            logger.info("Successfully write {} rows and {} batches", rows, batches);
        }
    }

    public void process(@NotNull Properties properties) throws SQLException, IOException {
        logger.info("[1] Start process");

        String url = properties.getProperty("url");

        try (Connection connection = DriverManager.getConnection(
                url,
                properties.getProperty("login"),
                properties.getProperty("password"))) {

            logger.info("[2] Read metadata from DB");

            DatabaseMetaData metadata = connection.getMetaData();

            String tableName = (String) properties.getOrDefault("tablename", null);
            String selectExpr = (String) properties.getOrDefault("selectexpr", null);

            ResultSet resultSetColumns = SQLUtils.getColumns(metadata, tableName);

            ResultSetMetaData resultSetColumnsMetaData = resultSetColumns.getMetaData();

            TypeDescription schema = TypeDescription.createStruct();

            logger.info("[3] Process type mapper");

            ITypeMapper genericTypeMapper = MapperManager.instance.detect(url);

            while (resultSetColumns.next()) {
                String columnName = resultSetColumns.getString("COLUMN_NAME");
                int dataType = resultSetColumns.getInt("DATA_TYPE");
                String typeName = resultSetColumns.getString("TYPE_NAME");
                int nullAllowed = resultSetColumns.getInt("NULLABLE");
                int columnSize = resultSetColumns.getInt("COLUMN_SIZE");

                logger.info("{} : dataType = {}, columnSize = {}, typeName = {}, nullAllowred = {}", columnName, dataType, columnSize, typeName, nullAllowed);
                // TODO: Pass length, precission, nullable
                TypeDescription typeDescription = genericTypeMapper.toOrc(typeName, Optional.of(columnSize), Optional.of(0), true);
                logger.info("typeDescription = {}", typeDescription);
                schema.addField(columnName, typeDescription);
            }

            resultSetColumns.close();

            String orcFilePath = properties.getProperty("orcfile");
            logger.info("[4] Export ORC file = {}", orcFilePath);
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            String userDir = System.getProperty("user.dir");
            logger.info("[4.3] User dir = {}", userDir);
            org.apache.hadoop.fs.Path currentDirPath = new org.apache.hadoop.fs.Path(userDir);
            logger.info("[5] currentDirPath = {}", currentDirPath);
            org.apache.hadoop.fs.FileSystem currentDirPathFileSystem = currentDirPath.getFileSystem(conf);
            logger.info("[6] fs.canonicalServName = {}", currentDirPathFileSystem.getCanonicalServiceName());
            currentDirPathFileSystem.delete(new Path(orcFilePath), false);
            logger.info("[6.1] create Orc-Writer with schema");
            try (Writer writer = OrcUtils.createWriter(currentDirPathFileSystem, orcFilePath, schema, properties)) {
                if (selectExpr != null) {
                    // TODO
                    throw new RuntimeException("Non implemented yet!");
                } else if (tableName != null) {
                    processWithWriter(writer, schema, connection, "SELECT * FROM " + tableName, 50_000);
                } else {
                    throw new IllegalArgumentException("Undefined table or SQL-expression for export!");
                }
            } catch (IOException ex) {
                logger.error("IOException : ", ex);
            }
            logger.info("[6.2] Orc-Writer closed");
        } catch (SQLException ex) {
            logger.error("SQLException : ", ex);
        }
        logger.info("[7] Finish!");
    }

    public void process(String[] args) throws SQLException, ParseException, IOException {

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
                properties.put("url", line.getOptionValue("u"));
                properties.put("login", line.getOptionValue("l"));
                properties.put("password", line.getOptionValue("p"));
                properties.put("orcfile", line.getOptionValue("o"));
                // Optional
                if (line.hasOption("t")) properties.put("tablename", line.getOptionValue("t"));
                if (line.hasOption("s")) properties.put("selectexpr", line.getOptionValue("s"));
                if (line.hasOption("co")) properties.put("orc.compression", line.getOptionValue("co"));
                if (line.hasOption("bf")) properties.put("orc.bloomColumns", line.getOptionValue("bf"));
                if (line.hasOption("ss")) properties.put("orc.stripesize", line.getOptionValue("ss"));
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
