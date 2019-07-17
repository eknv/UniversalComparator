package groovy

import groovy.io.FileType
import org.apache.maven.plugin.MojoExecutionException
import org.apache.maven.plugin.MojoFailureException
import org.apache.poi.hssf.usermodel.HSSFCellStyle
import org.apache.poi.hssf.usermodel.HSSFFont
import org.apache.poi.hssf.usermodel.HSSFRichTextString
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.hssf.util.HSSFColor
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellStyle
import org.apache.poi.ss.usermodel.DataFormat
import org.apache.poi.ss.usermodel.Font
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.ss.util.CellRangeAddress

import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.SQLException
import java.util.regex.Pattern


//////////////////////////////////////////////////////////////////////////////
// Classes and Utility methods

PrintTable printTable = new PrintTable(19, 2);
int row = 1;
printTable.addColumns(row++, 0, "failImmediate", "$failImmediate");
printTable.addColumns(row++, 0, "considerColumnMismatch", "$considerColumnMismatch");
printTable.addColumns(row++, 0, "considerColumnMissing", "$considerColumnMissing");
printTable.addColumns(row++, 0, "considerConstraintsNameMismatch", "$considerConstraintsNameMismatch");
printTable.addColumns(row++, 0, "considerConstraintsMissing", "$considerConstraintsMissing");
printTable.addColumns(row++, 0, "logDetails", "$logDetails");
printTable.addColumns(row++, 0, "logLevel", "$logLevel");
printTable.addColumns(row++, 0, "sources", "$sources");
printTable.addColumns(row++, 0, "projectRootFolder", "$projectRootFolder");
printTable.addColumns(row++, 0, "erdReExportToXml", "$erdReExportToXml");
printTable.addColumns(row++, 0, "erdProjectFilePath", "$erdProjectFilePath");
printTable.addColumns(row++, 0, "erdVisualParadigmXmlExportScript", "$erdVisualParadigmXmlExportScript");
printTable.addColumns(row++, 0, "executionDirectory", "$executionDirectory");
printTable.addColumns(row++, 0, "systemTables", "$systemTables");
printTable.addColumns(row++, 0, "tablesToIgnore", "$tablesToIgnore");
printTable.addColumns(row++, 0, "dbServerAddress", "$dbServerAddress");
printTable.addColumns(row++, 0, "dbSchemaName", "$dbSchemaName");
printTable.addColumns(row++, 0, "dbUserName", "$dbUserName");
printTable.addColumns(row++, 0, "dbPassword", "$dbPassword");
logger.info "The Comparison is being done with the following parameters: \n${printTable.asString(5)}"

new Params().init(failImmediate, considerColumnMismatch, considerColumnMissing, considerConstraintsNameMismatch,
        considerConstraintsMissing, logDetails,
        sources, projectRootFolder, erdReExportToXml, erdProjectFilePath, erdVisualParadigmXmlExportScript,
        executionDirectory, systemTables, tablesToIgnore, dbServerAddress, dbSchemaName, dbUserName, dbPassword)

public class Params {
    public static boolean FAIL_IMMEDIATE;
    public static boolean CONSIDER_COLUMN_MISMATCH;
    public static boolean CONSIDER_COLUMN_MISSING;
    public static boolean CONSIDER_CONSTRAINTS_NAME_MISMATCH;
    public static boolean CONSIDER_CONSTRAINTS_MISSING;
    public static boolean LOG_DETAILS;
    public static String PROJECT_ROOT_FOLDER;
    public static String ERD_PROJECT_FILE_PATH;
    public static boolean RE_EXPORT_ERD_XML;
    public static String Visual_PARADIGM_EXPORT_SCRIPT;
    public static String EXECUTION_DIRECTORY;
    public static Set<String> SYS_TABLES = new HashSet<>();
    public static Set<String> TABLES_TO_IGNORE = new HashSet<>();
    public static List<String> SOURCES = new ArrayList<>();
    public static Database database;

    public static boolean anyErrorOccured = false;
    public static int errorNumber = 1;

    /**
     * remove the line breaks from inside the properties
     */
    String removeNewLines(String newLines) {
        return newLines == null ? null : newLines.replace("\r", "").replace("\n", "").replaceAll(/\s+/, " ");
    }

    void init(boolean failImmediate,
            boolean considerColumnMismatch,
            boolean considerColumnMissing,
            boolean considerConstraintsNameMismatch,
            boolean considerConstraintsMissing,
            boolean logDetails,
            String sources,
            String projectRootFolder, 
            boolean erdReExportToXml,
            String erdProjectFilePath, 
            String erdVisualParadigmXmlExportScript, 
            String executionDirectory, 
            String systemTables, 
            String tablesToIgnore, 
            String dbServerAddress, 
            String dbSchemaName, 
            String dbUserName, 
            String dbPassword) {

        projectRootFolder = removeNewLines(projectRootFolder);
        erdProjectFilePath = removeNewLines(erdProjectFilePath);
        erdVisualParadigmXmlExportScript = removeNewLines(erdVisualParadigmXmlExportScript);
        executionDirectory = removeNewLines(executionDirectory);
        systemTables = removeNewLines(systemTables);
        tablesToIgnore = removeNewLines(tablesToIgnore);
        dbServerAddress = removeNewLines(dbServerAddress);


        FAIL_IMMEDIATE = failImmediate;
        CONSIDER_COLUMN_MISMATCH = considerColumnMismatch;
        CONSIDER_COLUMN_MISSING = considerColumnMissing;
        CONSIDER_CONSTRAINTS_NAME_MISMATCH = considerConstraintsNameMismatch;
        CONSIDER_CONSTRAINTS_MISSING = considerConstraintsMissing;
        LOG_DETAILS = logDetails;


        if (!Utils.isNullOrEmpty(sources)) {
            sources.split(",").each { source ->
                if (SOURCE.byName(source.trim().toUpperCase()) == null) {
                    throw new MojoExecutionException("The defined source $source is not supported. Valid sources are: ${SOURCE.values().join(",")}")
                }
                SOURCES.add(source.trim().toUpperCase());
            }
        }
        if (SOURCES.size() == 0) {
            throw new MojoExecutionException("At least one data source should be provided!");
        }

        PROJECT_ROOT_FOLDER = projectRootFolder;
        if ((Params.SOURCES.contains(SOURCE.HBM.toString()) || Params.SOURCES.contains(SOURCE.SQL.toString()))
                && (Utils.isNullOrEmpty(PROJECT_ROOT_FOLDER) || !new File(PROJECT_ROOT_FOLDER).exists())) {
            throw new MojoExecutionException("Project root folder $PROJECT_ROOT_FOLDER is not valid!");
        }

        ERD_PROJECT_FILE_PATH = erdProjectFilePath;
        if (Params.SOURCES.contains(SOURCE.ERD.toString())
                && (Utils.isNullOrEmpty(ERD_PROJECT_FILE_PATH) || !new File(ERD_PROJECT_FILE_PATH).exists())) {
            throw new MojoExecutionException("ERD project file $ERD_PROJECT_FILE_PATH is not valid!");
        }

        RE_EXPORT_ERD_XML = erdReExportToXml;

        Visual_PARADIGM_EXPORT_SCRIPT = erdVisualParadigmXmlExportScript;
        if (Params.SOURCES.contains(SOURCE.ERD.toString())
                && (Utils.isNullOrEmpty(Visual_PARADIGM_EXPORT_SCRIPT) || !new File(Visual_PARADIGM_EXPORT_SCRIPT).exists())) {
            throw new MojoExecutionException("Visual paradigm export script $Visual_PARADIGM_EXPORT_SCRIPT is not valid!");
        }

        EXECUTION_DIRECTORY = executionDirectory;
        if (Utils.isNullOrEmpty(EXECUTION_DIRECTORY) || !new File(EXECUTION_DIRECTORY).exists()) {
            EXECUTION_DIRECTORY = System.getProperty("user.dir");
        }

        if (!Utils.isNullOrEmpty(systemTables)) {
            systemTables.split(",").each { sysTable ->
                SYS_TABLES.add(sysTable.trim().toUpperCase());
            }
        }

        if (!Utils.isNullOrEmpty(tablesToIgnore)) {
            tablesToIgnore.split(",").each { tableToIgnoreProp ->
                TABLES_TO_IGNORE.add(tableToIgnoreProp.trim().toUpperCase());
            }
        }

        if (Params.SOURCES.contains(SOURCE.DB.toString())
                && (Utils.isNullOrEmpty(dbServerAddress) || Utils.isNullOrEmpty(dbSchemaName) || Utils.isNullOrEmpty(dbUserName) || Utils.isNullOrEmpty(dbPassword))
        ) {
            throw new MojoExecutionException("DB configuration parameters cannot be emtpty!");
        } else {
            database = new Database(dbSchemaName, dbUserName, dbPassword, dbServerAddress);
        }
    }
}


class PrintTable {
    String[][] rows;
    int rowCount;
    int columnCount;

    PrintTable(int rowCount, int columnCount) {
        this.rows = new String[rowCount][columnCount];
        this.rowCount = rowCount;
        this.columnCount = columnCount;
        for(int i = 0; i<rowCount; i++) {
            for(int j = 0; j<columnCount; j++) {
                rows[i][j] = "";
            }
        }
    }

    void add(int row, int column, String value) {
        rows[row-1][column-1] = value;
    }

    void addColumns(int row, int offset, Object... values) {
        values.eachWithIndex { value, index ->
            rows[row - 1][offset + index] = value;
        }
    }

    String asString(int padding) {
        def maxSizePerColumn = new HashMap<Integer, Integer>();
        for (int i = 0; i < rowCount; i++) {
            for (int j = 0; j < columnCount; j++) {
                String value = rows[i][j];
                int valueSize = value != null ? value.size() : 4; // 4 characters for null
                if (value != null &&
                        (maxSizePerColumn.get(j) == null || maxSizePerColumn.get(j) < valueSize)) {
                    maxSizePerColumn.put(j, valueSize);
                }
            }
        }

        def asString = new StringBuilder();

        for(int i = 0; i<rowCount; i++) {
            for(int j = 0; j<columnCount; j++) {
                String value = rows[i][j];
                asString.append(value);
                int valueSize = value != null ? value.size() : 4; // 4 characters for null
                int numberOfSpace = maxSizePerColumn.get(j) - valueSize + padding
                (0..numberOfSpace).each {
                    asString.append(" ")
                }
            }
            asString.append("\n")
        }

        asString.toString();
    }

}


class ExcelFile {

    Workbook workbook = new HSSFWorkbook()
    private Sheet sheet
    private int rowsCounter

    private Map<String, CellStyle> cellStyles = [:]
    private Map<String, Font> fonts = [:]

    /**
     * Creates a new workbook.
     *
     * @param the closure holds nested {@link ExcelFile} method calls
     * @return the created {@link Workbook}
     */
    Workbook workbook(Closure closure) {
        assert closure

        closure.delegate = this
        closure.call()
        workbook
    }

    void styles(Closure closure) {
        assert closure

        closure.delegate = this
        closure.call()
    }

    void data(Closure closure) {
        assert closure

        closure.delegate = this
        closure.call()
    }

    void commands(Closure closure) {
        assert closure

        closure.delegate = this
        closure.call()
    }

    void sheet(String name, int columnFreeze, int rowFreeze, Closure closure) {
        assert workbook

        assert name
        assert closure

        sheet = workbook.createSheet(name)
        rowsCounter = 0

        sheet.createFreezePane(columnFreeze, rowFreeze);

        closure.delegate = sheet
        closure.call()
    }

    void cellStyle(String cellStyleId, Closure closure) {
        assert workbook

        assert cellStyleId
        assert !cellStyles.containsKey(cellStyleId)
        assert closure

        CellStyle cellStyle = workbook.createCellStyle()
        cellStyles.put(cellStyleId, cellStyle)

        closure.call(cellStyle)
    }

    void font(String fontId, Closure closure) {
        assert workbook

        assert fontId
        assert !fonts.containsKey(fontId)
        assert closure

        Font font = workbook.createFont()
        fonts.put(fontId, font)

        closure.call(font)
    }

    void applyCellStyle(Map<String, Object> args) {
        assert workbook

        def cellStyleId = args.cellStyle
        def fontId = args.font
        def dataFormat = args.dataFormat

        def sheetName = args.sheet

        def rows = args.rows ?: -1          // -1 denotes all rows
        def cells = args.columns ?: -1      // -1 denotes all cols

        def colName = args.columnName

        assert cellStyleId || fontId || dataFormat

        assert rows && (rows instanceof Number || rows instanceof Range<Number>)
        assert cells && (cells instanceof Number || cells instanceof Range<Number>)

        if (cellStyleId && !cellStyles.containsKey(cellStyleId)) cellStyleId = null
        if (fontId && !fonts.containsKey(fontId)) fontId = null
        if (dataFormat && !(dataFormat instanceof String)) dataFormat = null
        if (sheetName && !(sheetName instanceof String)) sheetName = null
        if (colName && !(colName instanceof String)) colName = null

        def sheet = sheetName ? workbook.getSheet(sheetName as String) : workbook.getSheetAt(0)
        assert sheet

        if (rows == -1) rows = [1..rowsCounter]
        if (rows instanceof Number) rows = [rows]

        rows.each { Number rowIndex ->
            assert rowIndex

            Row row = sheet.getRow(rowIndex.intValue() - 1)
            if (!row) return

            if (cells == -1) cells = [row.firstCellNum..row.lastCellNum]
            if (rows instanceof Number) rows = [rows]

            def applyStyleFunc = { Number cellIndex ->
                assert cellIndex

                Cell cell = row.getCell(cellIndex.intValue() - 1)
                if (!cell) return
                // do not apply the errorColumn style if the cell is empty
                if (cellStyleId
                        && (!"errorColumn".equals(cellStyleId) || !"${cell.getStringCellValue()}".toString().trim().equals(""))
                ) {
                    cell.setCellStyle(cellStyles.get(cellStyleId));
                }
                if (fontId) cell.getCellStyle().setFont(fonts.get(fontId))
                if (dataFormat) {
                    DataFormat df = workbook.createDataFormat()
                    cell.getCellStyle().setDataFormat(df.getFormat(dataFormat as String))
                }
            }

            cells.each applyStyleFunc
        }
    }


    void autSize(Map<String, Object> args) {
        assert workbook
        def cols = args.columns
        def sheetName = args.sheet
        assert cols && (cols instanceof Number || cols instanceof Range<Number>)
        if (cols instanceof Number) cols = [cols]
        if (sheetName && !(sheetName instanceof String)) sheetName = null
        def sheet = sheetName ? workbook.getSheet(sheetName as String) : workbook.getSheetAt(0)
        cols.each { col ->
            sheet.autoSizeColumn(col-1);
        }
    }


    void mergeCells(Map<String, Object> args) {
        assert workbook

        def rows = args.rows
        def cols = args.columns
        def sheetName = args.sheet

        assert rows && (rows instanceof Number || rows instanceof Range<Number>)
        assert cols && (cols instanceof Number || cols instanceof Range<Number>)

        if (rows instanceof Number) rows = [rows]
        if (cols instanceof Number) cols = [cols]
        if (sheetName && !(sheetName instanceof String)) sheetName = null

        def sheet = sheetName ? workbook.getSheet(sheetName as String) : workbook.getSheetAt(0)

        sheet.addMergedRegion(new CellRangeAddress(rows.first() - 1, rows.last() - 1, cols.first() - 1, cols.last() - 1))
    }

    void applyColumnWidth(Map<String, Object> args) {
        assert workbook

        def cols = args.columns
        def sheetName = args.sheet
        def width = args.width

        assert cols && (cols instanceof Number || cols instanceof Range<Number>)
        assert width && width instanceof Number

        if (cols instanceof Number) cols = [cols]
        if (sheetName && !(sheetName instanceof String)) sheetName = null

        def sheet = sheetName ? workbook.getSheet(sheetName as String) : workbook.getSheetAt(0)

        cols.each {
            sheet.setColumnWidth(it - 1, width.intValue())
        }
    }

    void header(List<String> names) {
        assert sheet
        assert names

        Row row = sheet.createRow(rowsCounter++ as int)
        names.eachWithIndex { String value, col ->
            Cell cell = row.createCell(col)
            cell.setCellValue(value)
        }
    }

    void emptyRow() {
        assert sheet

        sheet.createRow(rowsCounter++ as int)
    }

    void row(values) {
        assert sheet
        assert values

        Row row = sheet.createRow(rowsCounter++ as int)
        values.eachWithIndex { value, col ->
            Cell cell = row.createCell(col)
            switch (value) {
                case Date: cell.setCellValue((Date) value); break
                case Double: cell.setCellValue((Double) value); break
                case BigDecimal: cell.setCellValue(((BigDecimal) value).doubleValue()); break
                case Number: cell.setCellValue(((Number) value).doubleValue()); break
                default:
                    def stringValue = value?.toString() ?: ""
                    if (stringValue.startsWith('=')) {
                        cell.setCellType(Cell.CELL_TYPE_FORMULA)
                        cell.setCellFormula(stringValue.substring(1))
                    } else {
                        cell.setCellValue(new HSSFRichTextString(stringValue))
                    }
                    break
            }
        }
    }

    int getRowCount() {
        assert sheet

        rowsCounter
    }
}

class Database {

    private static final String JDBC_DRIVER = "com.ibm.as400.access.AS400JDBCDriver";

    public final String user;
    public final String password;
    public final String url;
    public final String schemaName;


    private Database(String schemaName, String user, String password, String url) {
        this.schemaName = schemaName
        this.user = user;
        this.password = password;
        this.url = url;
    }

    public groovy.sql.Sql newSql() {

        return groovy.sql.Sql.newInstance("jdbc:as400://${url}/${schemaName};prompt=false;naming=sql;errors=full;date format=usa;date separator=/;time format=hms;time separator=:",
                user, password, Database.JDBC_DRIVER)
    }

    public DatabaseMetaData getMetaData() {
        return newSql().connection.metaData
    }

    public Map<String, Table> getTables() {
        return getDBTables(schemaName, getMetaData());
    }

    private Map<String, Table> getDBTables(String schema, DatabaseMetaData metaData) throws SQLException {
        ResultSet columnsResultSet = metaData.getColumns(null, schema, null, null);
        try {
            final Map<String, Table> tables = new TreeMap<String, Table>();
            while (columnsResultSet.next()) {
                final String tableName = columnsResultSet.getString(3).trim().toUpperCase();
                if (Params.TABLES_TO_IGNORE.contains(tableName) || Params.SYS_TABLES.contains(tableName)) {
                    continue;
                }
                final String columnName = columnsResultSet.getString(4);
                final String columnType = columnsResultSet.getString(6);
                final int columnSize = columnsResultSet.getInt(7);
                final int decimalDigits = columnsResultSet.getInt(9);
                final boolean nullable = columnsResultSet.getInt(11);
                Table dbTable = tables.get(tableName);
                if (dbTable == null) {
                    dbTable = getTableInfo(schema, tableName, metaData);
                    dbTable.updateColumnsUniqueFlags();
                    tables.put(tableName, dbTable);
                }
                Column column = new Column();
                dbTable.addColumn(column);
                column.name = "$columnName".toString().toUpperCase().trim();
                column.type = getDBDataType(tableName, column.name, "${columnType}".trim().toUpperCase());
                column.size = Utils.getColumnSize(column.type, "${columnSize}", "${decimalDigits}");
                column.nullable = "${nullable}".toString().equalsIgnoreCase("true") ? true : false;
                column.isId = dbTable.primaryKeys.contains(column.name);
                if (dbTable.isColumnUsedInSingleUniqueConstraints(column.name)
                        || dbTable.primaryKeys.contains(column.name)) {
                    column.isUnique = true;
                } else {
                    column.isUnique = false;
                }


            }
            return tables
        } finally {
            closeQuietly(columnsResultSet);
        }
    }


    private TYPE getDBDataType(String tableName, String columnName, String type) {
        type = type.trim().toUpperCase();
        if (type.equals("TIMESTAMP")) {
            return TYPE.TIMESTAMP;
        } else if (type.equals("VARGRAPHIC")) {
            return TYPE.VARGRAPHIC;
        } else if (type.equals("INTEGER")) {
            return TYPE.INTEGER;
        } else if (type.equals("DECIMAL")) {
            return TYPE.DECIMAL;
        } else if (type.equals("SMALLINT")) {
            return TYPE.SMALLINT;
        } else if (type.equals("BIGINT")) {
            return TYPE.BIGINT;
        } else if (type.equals("DATE")) {
            return TYPE.DATE;
        } else if (type.equals("TIME")) {
            return TYPE.TIME;
        } else if (type.equals("CLOB")) {
            return TYPE.CLOB;
        } else if (type.equals("BLOB")) {
            return TYPE.BLOB;
        } else {
            throw new MojoExecutionException("The type '$type' for the column ${tableName}.${columnName} could not be recognized");
        }
    }


    private Table getTableInfo(String schema, String tableName, DatabaseMetaData metaData) throws SQLException {
        Table dbTable = new Table();
        dbTable.name = tableName;
        dbTable.primaryKeys = getPrimaryKeys(schema, tableName, metaData);
        dbTable.foreignKeys = getForeignKeys(schema, tableName, metaData);
        setUniqueConstraints(schema, dbTable, metaData);
        return dbTable;
    }


    private Set<String> getPrimaryKeys(String schema, String tableName, DatabaseMetaData metaData) throws SQLException {
        final HashSet<String> primaryKeyColumns = new HashSet<String>(2);
        final ResultSet primaryKeysRS = metaData.getPrimaryKeys(null, schema, tableName);
        try {
            while (primaryKeysRS.next()) {
                primaryKeyColumns.add(primaryKeysRS.getString(4));
            }
            return primaryKeyColumns;
        } finally {
            closeQuietly(primaryKeysRS);
        }
    }


    private Set<ForeignKey> getForeignKeys(String schema, String tableName, DatabaseMetaData metaData) throws SQLException {
        def foreignKeys = new HashSet<ForeignKey>();
        ResultSet foreignKeysRS = metaData.getImportedKeys(null, schema, tableName);
        try {
            while (foreignKeysRS.next()) {
                ForeignKey foreignKey = new ForeignKey();
                foreignKey.name = "${foreignKeysRS.getString(12)}".toUpperCase().trim();
                foreignKey.tableName = tableName;
                foreignKey.columnName = "${foreignKeysRS.getString(8)}".toUpperCase().trim();
                foreignKey.foreignTableName = "${foreignKeysRS.getString(3)}".toUpperCase().trim();
                foreignKeys.add(foreignKey);
            }
            return foreignKeys;
        } finally {
            closeQuietly(foreignKeysRS);
        }
    }


    private void setUniqueConstraints(String schema, Table table, DatabaseMetaData metaData) throws SQLException {
        ResultSet uniqueConstraintsRS = metaData.getIndexInfo(null, schema, table.name, true, false);
        try {
            def uniqueConstraintsMap = new HashMap<String, Set<String>>();
            while (uniqueConstraintsRS.next()) {
                String constraintName = "${uniqueConstraintsRS.getString(6)}".toUpperCase().trim();
                String IndexColumnName = "${uniqueConstraintsRS.getString(9)}".toUpperCase().trim();
                Set constraints = uniqueConstraintsMap.get(constraintName);
                if (constraints == null) {
                    constraints = new HashSet();
                    uniqueConstraintsMap.put(constraintName, constraints);
                }
                constraints.add(IndexColumnName);
            }

            uniqueConstraintsMap.each { constraintName, constraints ->
                UniqueConstraint uniqueConstraint = new UniqueConstraint();
                uniqueConstraint.tableName = table.name;
                constraints.each {
                    uniqueConstraint.addColumns(it);
                }
                if (constraints.size() > 1) {
                    table.addUniqueConstraints(uniqueConstraint);
                } else {
                    table.addSingleUniqueConstraints(uniqueConstraint);
                }
            }
        } finally {
            closeQuietly(uniqueConstraintsRS);
        }
    }


    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ignoredCloseException) {
                // ignore exception when closing
            }
        }
    }

    public String toString() {
        return "Database (user=${user}, schema=${schemaName}, url=" + abbreviate(url, 40) + ") "
    }

    private static String abbreviate(String str, int maxWidth) {
        return str.length() <= maxWidth ? str :
                str.substring(0, maxWidth) + "...";
    }
}


class Utils {
    public static boolean isNullOrEmpty(object) {
        return object == null || object.toString().trim().equals('');
    }

    public static String getColumnSize(TYPE type, String length, String scale) {
        if (type == TYPE.TIMESTAMP || TYPE == TYPE.DATE || TYPE == TYPE.TIME) {
            return null;
        } else if (type == TYPE.DECIMAL) {
            if (scale.trim().equals("0")) {
                return length.trim();
            } else {
                return "${length.trim()}.${scale.trim()}"
            }
        } else if (type == TYPE.INTEGER) {
            return length.trim();
        } else if (type == TYPE.BIGINT) {
            return "19";
        } else if (type == TYPE.SMALLINT) {
            return length.trim();
        } else if (type == TYPE.VARGRAPHIC) {
            return length.trim();
        } else if (type == TYPE.CLOB) {
            return length.trim();
        } else if (type == TYPE.BLOB) {
            return length.trim();
        }
    }
}


enum TYPE {
    TIMESTAMP, DATE, TIME, DECIMAL, INTEGER, BIGINT, SMALLINT, VARGRAPHIC, VARCHAR, CLOB, BLOB
}


enum SOURCE {
    ERD, HBM, DB, SQL;

    public static SOURCE byName(String name) {
        return values().find { source ->
            source.toString().equalsIgnoreCase(name);
        }
    }
}

enum XLS_COLUMN {
    NAME(0), TYPE(1), SIZE(2), NULLABLE(3), UNIQUE(4), PRIMARY_KEY(5);
    int columnFactor;

    XLS_COLUMN(int columnFactor) {
        this.columnFactor = columnFactor;
    }
}


class SqlFragment {
    public static String CREATE_TABLE = "CREATE TABLE";
    public static String ALTER_TABLE = "ALTER TABLE";
    public static String ADD_CONSTRAINT = "ADD CONSTRAINT";
    public static String REFERENCES = "REFERENCES";
    public static String ALTER_COLUMN = "ALTER COLUMN";
    public static String DROP_NOT_NULL = "DROP NOT NULL";
    public static String SET_NOT_NULL = "SET NOT NULL";
    public static String FOREIGN_KEY = "FOREIGN KEY";
    public static String PRIMARY_KEY = "PRIMARY KEY";
    public static String UNIQUE = "UNIQUE";
    public static String NOT_NULL = "NOT NULL";
    public static String ADD_COLUMN = "ADD COLUMN";
    public static String SET_DATA_TYPE = "SET DATA TYPE";
    public static String DROP_COLUMN = "DROP COLUMN";
    public static String SPACE = " ";
    public static String PARENTHESIS_OPEN = "(";
    public static String PARENTHESIS_CLOSE = ")";
    public static final String SEPARATOR = ";";
}

class Column {
    String name;
    TYPE type;
    String size;
    Boolean nullable;
    Boolean isId;
    Boolean isUnique;
    // visual paradigm ID
    String vpId;

    String toString() {
        "(name:$name type:$type size:$size nullable:$nullable isId:$isId isUnique:$isUnique)"
    }
}

class ForeignKey implements Comparable<ForeignKey> {
    String name;
    String tableName;
    String columnName;
    String foreignTableName;
    String foreignClassName;

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    boolean equals(Object obj) {
        if (!(obj instanceof ForeignKey)) return false;
        ForeignKey foreignKey = (ForeignKey) obj;
        return this.toString().equalsIgnoreCase(foreignKey.toString());
    }

    @Override
    int compareTo(ForeignKey o) {
        if (o == null) return -1;
        return toString().compareTo(o.toString());
    }

    String toString() {
        "${tableName}.${columnName}->${foreignTableName}".toString().toUpperCase().trim()
    }
}

class UniqueConstraint implements Comparable<UniqueConstraint> {
    String name;
    Set<String> columnNames = new TreeSet();
    String tableName;

    void addColumns(String column) {
        if ((column.contains(","))) {
            String[] columns = column.split(",");
            columns.each {
                columnNames.add(it.toUpperCase().trim());
            }
        } else {
            columnNames.add(column);
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    boolean equals(Object obj) {
        if (!(obj instanceof UniqueConstraint)) return false;
        UniqueConstraint uniqueConstraint = (UniqueConstraint) obj;
        return this.toString().equalsIgnoreCase(uniqueConstraint.toString());
    }

    @Override
    int compareTo(UniqueConstraint o) {
        if (o == null) return -1;
        return toString().compareTo(o.toString());
    }

    String toString() {
        "${new ArrayList(columnNames).join(",")}".toString().replaceAll(/\s/, "")
    }
}

class Table {
    String name;
    String className;
    List<Column> columns = []
    Set<String> primaryKeys = []
    Set<ForeignKey> foreignKeys = []
    Set<UniqueConstraint> uniqueConstraints = []
    Set<UniqueConstraint> singleUniqueConstraints = []

    void addColumn(Column c) {
        columns << c
    }

    Map getColumnsAsMap() {
        Map<String, Column> columnsMap = new HashMap<>();
        columns.each { column ->
            columnsMap.put(column.name, column);
        }
        columnsMap
    }

    void addForeign(ForeignKey fk) {
        foreignKeys << fk
    }

    void addUniqueConstraints(UniqueConstraint uniqueConstraint) {
        uniqueConstraints << uniqueConstraint
    }

    void addSingleUniqueConstraints(UniqueConstraint uniqueConstraint) {
        singleUniqueConstraints << uniqueConstraint
    }

    void removeColumnByName(String name) {
        columns.removeAll {
            it.name != null && it.name.trim().equalsIgnoreCase(name == null ? null : name.trim());
        }
    }

    Column getColumnByName(String name) {
        Column primaryKey = columns.find {
            if (it.name.trim().equalsIgnoreCase(name != null ? name.trim() : null)) {
                return it;
            }
        }
        return primaryKey;
    }

    UniqueConstraint getUniqueKeyByName(String name) {
        UniqueConstraint uniqueKey = uniqueConstraints.find {
            if (it.name.trim().equalsIgnoreCase(name != null ? name.trim() : null)) {
                return it;
            }
        }
        return uniqueKey;
    }

    boolean isColumnUsedInUniqueConstraints(String columnName) {
        return uniqueConstraints.any { uniqueKey ->
            return uniqueKey.columnNames.any {
                it.trim().equalsIgnoreCase(columnName != null ? columnName : null);
            }
        }
    }

    boolean isColumnUsedInSingleUniqueConstraints(String columnName) {
        return singleUniqueConstraints.any { singleUniqueKey ->
            return singleUniqueKey.columnNames.any {
                it.trim().equalsIgnoreCase(columnName != null ? columnName : null);
            }
        }
    }

    void updateColumnsUniqueFlags() {
        columns.each { column ->
            column.isUnique = column.isUnique || isColumnUsedInUniqueConstraints(column.name);
        }
    }

    String toString() {
        StringBuilder sb = new StringBuilder()
        sb.append(name);
        sb.append("\n\t\t\tColumns: [")
        columns.each {
            sb.append(it.toString()).append(",")
        }
        sb.append("]")

        sb.append("\n\t\t\tForeignKeys: [")
        foreignKeys.each {
            sb.append(it.toString()).append(",")
        }
        sb.append("]")

        sb.append("\n\t\t\tUniqueKeys: [")
        uniqueConstraints.each {
            sb.append(it.toString()).append(",")
        }
        sb.append("]")

        sb.toString()
    }
}


def TYPE getColumnType(String columnStatement) {
    if (columnStatement.contains("TIMESTAMP")) {
        return TYPE.TIMESTAMP;
    } else if (columnStatement.contains("BIGINT")) {
        return TYPE.BIGINT;
    } else if (columnStatement.contains("INTEGER")) {
        return TYPE.INTEGER;
    } else if (columnStatement.contains("SMALLINT")) {
        return TYPE.SMALLINT;
    } else if (columnStatement.contains("CLOB")) {
        return TYPE.CLOB;
    } else if (columnStatement.contains("BLOB")) {
        return TYPE.BLOB;
    } else if (columnStatement.contains("VARGRAPHIC")) {
        if (!columnStatement.contains("CCSID 1200")) {
            throw new MojoExecutionException("CCSID missing in the statement '${columnStatement}'");
        }
        return TYPE.VARGRAPHIC;
    } else if (columnStatement.contains("DECIMAL")) {
        return TYPE.DECIMAL;
    } else if (columnStatement.contains("DATE")) {
        return TYPE.DATE;
    } else {
        throw new MojoExecutionException("The statement '${columnStatement}' could not be processed");
    }
}


def void addUpdateTableColumn(Map<String, Table> tableMap, String tableName, Column column) {
    Table sqlTable = tableMap.get(tableName);
    if (sqlTable == null) {
        sqlTable = new Table();
        sqlTable.name = tableName;
        sqlTable.addColumn(column);
        tableMap.put(tableName, sqlTable);
    } else {
        Column existingColumn = sqlTable.getColumnByName(column.name);
        if (existingColumn == null) {
            sqlTable.addColumn(column);
        } else {
            if (column.name != null) {
                existingColumn.name = column.name;
            }
            if (column.isId != null) {
                existingColumn.isId = column.isId;
            }
            if (column.isUnique != null) {
                existingColumn.isUnique = column.isUnique;
            }
            if (column.nullable != null) {
                existingColumn.nullable = column.nullable;
            }
            if (column.size != null) {
                existingColumn.size = column.size;
            }
            if (column.type != null) {
                existingColumn.type = column.type;
            }
        }
    }
}

def exit(int exitStatus, String errorMessage) {
    System.err << "=".multiply(65) + "\n${errorMessage}\n" + "=".multiply(65)
    System.exit(exitStatus)
}


def TYPE getERDType(String type) {
    if (type == null) {
        throw new MojoExecutionException("Type cannot be null!");
    }
    String dataType = type.toUpperCase().trim();
    if ("BIGINT".equals(dataType)) {
        return TYPE.BIGINT;
    } else if ("BLOB".equals(dataType)) {
        return TYPE.BLOB;
    } else if ("CLOB".equals(dataType)) {
        return TYPE.CLOB;
    } else if ("DATE".equals(dataType)) {
        return TYPE.DATE;
    } else if ("DECIMAL".equals(dataType)) {
        return TYPE.DECIMAL;
    } else if ("INTEGER".equals(dataType)) {
        return TYPE.INTEGER;
    } else if ("SMALLINT".equals(dataType)) {
        return TYPE.SMALLINT;
    } else if ("TIME".equals(dataType)) {
        return TYPE.TIME;
    } else if ("TIMESTAMP".equals(dataType)) {
        return TYPE.TIMESTAMP;
    } else if ("VARCHAR".equals(dataType)) {
        return TYPE.VARGRAPHIC;
    } else if ("VARGRAPHIC".equals(dataType)) {
        return TYPE.VARGRAPHIC;
    } else {
        throw new MojoExecutionException("The provided type is not valid: $dataType");
    }
}


def void processErdProjectFile(Map<String, Table> erdTableMap, boolean reload) {
    def outputLocation = Params.EXECUTION_DIRECTORY;
    new File(outputLocation).mkdirs();
    def vpProjectFileLocation = "${outputLocation}${System.getProperty("file.separator")}project.xml"
    if (!new File(vpProjectFileLocation).exists() || reload) {
        def erdProjectFile = new File(Params.ERD_PROJECT_FILE_PATH);
        def xmlExportConfigArguments = [
                "-project", erdProjectFile,
                "-out", outputLocation,
                "-refmodel", "true",
                "-simple",
                "-noimage"]
        def commandToExecute = [Params.Visual_PARADIGM_EXPORT_SCRIPT] + xmlExportConfigArguments;
        def scriptDirectoryPath = "${Params.Visual_PARADIGM_EXPORT_SCRIPT.substring(0, Params.Visual_PARADIGM_EXPORT_SCRIPT.lastIndexOf("/"))} ";
        // Massage command to a format ProcessBuilder can take
        // GString --> String to avoid mixing String and GString otherwise ProcessBuilder array copy fails
        commandToExecute = commandToExecute
                .collect { s -> s.toString() }
                .collect { s -> "\\" == System.getProperty("file.separator") ? s.replaceAll('/', '\\\\') : s }
        logger.info "<ERD> Preparing to invoke Visual Paradigm XML Export CLI command using the following configuration"
        logger.debug "<ERD> Command and its arguments: ${commandToExecute.join("\t")}"
        logger.info "<ERD> Process will be started in the following working directory: \t${scriptDirectoryPath} "
        logger.info "<ERD> Now launching external Visual Paradigm process... "
        def process = new ProcessBuilder(commandToExecute)
                .redirectErrorStream(true)
                .directory(new File(scriptDirectoryPath))
                .start()
        process.in.eachLine { line -> logger.debug "<ERD>\t[>>] $line" }
        def returnCodeOfProcess = process.waitFor()
        if (returnCodeOfProcess != 0) {
            throw new MojoExecutionException("FAILED: Abnormal termination of Visual Paradigm export")
        }
        logger.info "<ERD> Visual Paradigm CLI Export has been successful"
        if (!new File(vpProjectFileLocation).exists()) {
            throw new MojoExecutionException("FAILED: Could not find file exported by VP CLI tool: $vpProjectFileLocation \nCheck debug logs for any errors: see lines prefixed with [>>]!")
        }
        logger.info "<ERD> Processing Visual Paradigm export: ${vpProjectFileLocation}"
    }

    def projects = new XmlSlurper().parse(vpProjectFileLocation);

    def tableNameToTableElementMap = projects.'**'
            .findAll { node ->
        String tableName = node.@Name.toString().toUpperCase().trim();
        node.name() == 'DBTable' && node.@"DataModel" == "Physical" && !Params.TABLES_TO_IGNORE.contains(tableName) && !Params.SYS_TABLES.contains(tableName)
    }.collectEntries { dbTableElement ->
        [(dbTableElement.@Name.toString().toUpperCase().trim()): dbTableElement]
    }

    Map<String, String> vpColumnId2TableName = new HashMap<>();
    tableNameToTableElementMap.each { tableName, node ->
        Table table = new Table();
        table.name = tableName;
        node.ModelChildren.DBColumn.each {
            vpColumnId2TableName.put("${it.@Id}".toString().trim(), tableName);
        }
    }

    tableNameToTableElementMap.each { tableName, node ->

        Table erdTable = new Table();
        erdTable.name = tableName;

        node.Constraints.DBUniqueConstraint.each { dbUniqueConstraint ->
            UniqueConstraint uniqueConstraint = new UniqueConstraint();
            uniqueConstraint.name = "${dbUniqueConstraint.@Name}"
            dbUniqueConstraint.Columns.DBColumn.each { dbColumn ->
                uniqueConstraint.addColumns("${dbColumn.@Name}");
            }
            erdTable.addUniqueConstraints(uniqueConstraint);
        }

        node.ModelChildren.DBColumn.each {

            Column column = new Column();
            column.name = it.@Name;
            if (!Utils.isNullOrEmpty("${it.@PrimaryKey}".toString())) {
                if ("true".equalsIgnoreCase("${it.@PrimaryKey}".toString())) {
                    column.isId = true;
                } else if ("false".equalsIgnoreCase("${it.@PrimaryKey}".toString())) {
                    column.isId = false;
                }
            }
            if (!Utils.isNullOrEmpty("${it.@Unique}".toString())) {
                if ("true".equalsIgnoreCase("${it.@Unique}".toString())) {
                    column.isUnique = true;
                } else if ("false".equalsIgnoreCase("${it.@Unique}".toString())) {
                    column.isUnique = false;
                }
            }
            if (!Utils.isNullOrEmpty("${it.@Nullable}".toString())) {
                if ("true".equalsIgnoreCase("${it.@Nullable}".toString())) {
                    column.nullable = true;
                } else if ("false".equalsIgnoreCase("${it.@Nullable}".toString())) {
                    column.nullable = false;
                }
            }

            column.type = getERDType("${it.@Type}".toString());
            column.size = "${Utils.getColumnSize(column.type, "${it.@Length}", "${it.@Scale}")}";
            if (TYPE.SMALLINT == column.type && column.size.toString().trim().equals("0")) {
                column.size = "5";
            }
            erdTable.addColumn(column);

            if (!Utils.isNullOrEmpty("${it.ForeignKeyConstraints.DBForeignKeyConstraint.@RefColumn}".toString())) {
                ForeignKey foreignKey = new ForeignKey();
                foreignKey.tableName = erdTable.name;
                foreignKey.columnName = column.name;
                foreignKey.foreignTableName = vpColumnId2TableName.get("${it.ForeignKeyConstraints.DBForeignKeyConstraint.@RefColumn[0]}".toString().trim());
                if (Integer.valueOf("${it.ForeignKeyConstraints.DBForeignKeyConstraint.@RefColumn.size()}") > 1) {
                    logger.warn "The foreign-key (${tableName}.${column.name}->${foreignKey.tableName}) " +
                            "has been defined ${it.ForeignKeyConstraints.DBForeignKeyConstraint.@RefColumn.size()} times!"
                }
                erdTable.addForeign(foreignKey);
            }
        }

        erdTable.updateColumnsUniqueFlags();

        erdTableMap.put(tableName, erdTable);
    }

    erdTableMap
}

/**
 * this method gathers sql information out of the given statement and put it in the given sqlTableMap variable
 * @param statement
 * @param sqlTableMap
 */
def Table processSQLStatement(String statement, Map<String, Table> sqlTableMap) {

    statement = statement.toUpperCase().replaceAll(/\s+/, " ");
    // change the formatting of the data-type sizes
    statement = statement.replaceAll(/\(\s?(\d+),\s?(\d+)\s?\)/, /\[$1_$2\]/);
    statement = statement.replaceAll(/\(\s?(\d+)\s?\)/, /\[$1\]/);
    statement = statement.replaceAll(/PRIMARY KEY\s?\(([\w|\s|,]*)\)/, /PRIMARY KEY\{$1\}/);
    statement = statement.replaceAll(/UNIQUE\s?\(([\w|\s|,]*)\)/, /UNIQUE\<$1\>/);

    if (statement.contains(SqlFragment.CREATE_TABLE)) {

        int startIndex = statement.indexOf(SqlFragment.CREATE_TABLE) + SqlFragment.CREATE_TABLE.length();
        int endIndex = statement.indexOf(SqlFragment.PARENTHESIS_OPEN, startIndex + 1);
        tableName = statement.substring(startIndex, endIndex).toUpperCase().trim();

        if (Params.TABLES_TO_IGNORE.contains(tableName) || Params.SYS_TABLES.contains(tableName)) {
            return;
        }

        Table sqlTable = sqlTableMap.get(tableName);
        if (sqlTable == null) {
            sqlTable = new Table();
            sqlTable.name = tableName;
            sqlTableMap.put(tableName, sqlTable);
        }

        String primaryKeyName = null;
        if (statement.contains(SqlFragment.PRIMARY_KEY)) {
            primaryKeyName = statement.substring(statement.indexOf("{") + 1, statement.indexOf("}"));
            String primaryKeyStatement = statement.substring(statement.indexOf(SqlFragment.PRIMARY_KEY), statement.indexOf("}") + 1);
            statement = statement.replace(primaryKeyStatement, "");
        }

        if (statement.contains(SqlFragment.UNIQUE + "<")) {
            uniqueKeyNames = statement.substring(statement.indexOf("<") + 1, statement.indexOf(">"));
            String uniqueKeyStatement = statement.substring(statement.indexOf(SqlFragment.UNIQUE), statement.indexOf(">") + 1);
            statement = statement.replace(uniqueKeyStatement, "");
            UniqueConstraint uniqueKey = new UniqueConstraint();
            uniqueKey.addColumns(uniqueKeyNames);
            sqlTable.addUniqueConstraints(uniqueKey);
        }

        String[] columnStatements = statement.substring(statement.indexOf("(") + 1, statement.indexOf(")")).split(",");
        columnStatements.each {
            String columnStatement = it.toUpperCase().trim();
            if (columnStatement.trim().isEmpty()) {
                return;
            }
            String columnName = columnStatement.substring(0, columnStatement.indexOf(SqlFragment.SPACE));
            Column column = sqlTable.getColumnByName(columnName);
            if (column == null) {
                column = new Column();
                column.name = columnName;
                column.nullable = true;
                column.isId = false;
                column.isUnique = false;
                sqlTable.addColumn(column);
            }
            if (columnStatement.contains("[")) {
                String length = columnStatement.substring(columnStatement.indexOf("[") + 1, columnStatement.indexOf("]"))
                column.size = length == null ? null : length.trim().replaceAll("_", ".").replace(".0", "");
            }
            column.type = getColumnType(columnStatement);
            if (columnStatement.contains(SqlFragment.UNIQUE)) {
                column.isUnique = true;
            }
            if (columnStatement.contains(SqlFragment.NOT_NULL)) {
                column.nullable = false;
            }
        }

        if (primaryKeyName != null) {
            primaryKeyName.split(",").each {
                Column primaryKey = sqlTable.getColumnByName(it.trim());
                primaryKey.isId = true;
                primaryKey.isUnique = true;
            }
        }

        sqlTable.updateColumnsUniqueFlags();

    } else if (statement.contains(SqlFragment.ALTER_TABLE)) {
        int startIndex = statement.indexOf(SqlFragment.ALTER_TABLE) + SqlFragment.ALTER_TABLE.length();
        int endIndex = statement.indexOf(SqlFragment.SPACE, startIndex + 1);
        tableName = statement.substring(startIndex, endIndex).toUpperCase().trim();

        if (Params.TABLES_TO_IGNORE.contains(tableName) || Params.SYS_TABLES.contains(tableName)) {
            return;
        }

        if (statement.contains(SqlFragment.DROP_NOT_NULL)) {
            // ALTER TABLE AC_USER ALTER COLUMN AD_PERSON DROP NOT NULL;
            String columnName = statement.substring(statement.indexOf(SqlFragment.ALTER_COLUMN) + SqlFragment.ALTER_COLUMN.length(),
                    statement.indexOf(SqlFragment.DROP_NOT_NULL)).trim();
            Column _column = new Column();
            _column.name = columnName;
            _column.nullable = true;
            addUpdateTableColumn(sqlTableMap, tableName, _column);
        } else if (statement.contains(SqlFragment.SET_NOT_NULL)) {
            // ALTER TABLE AD_PAYCNDTN ALTER COLUMN APPINSTANCE SET NOT NULL;
            String columnName = statement.substring(statement.indexOf(SqlFragment.ALTER_COLUMN) + SqlFragment.ALTER_COLUMN.length(),
                    statement.indexOf(SqlFragment.SET_NOT_NULL)).trim();
            Column _column = new Column();
            _column.name = columnName;
            _column.nullable = false;
            addUpdateTableColumn(sqlTableMap, tableName, _column);
        } else if (statement.contains(SqlFragment.FOREIGN_KEY)) {
            //         ALTER TABLE AG_SM_AGRMNT ADD CONSTRAINT FK_AG_SM_AGRMNT_PR_CRNCY FOREIGN KEY (PR_CRNCY) REFERENCES PR_CRNCY;
            String foreignKeyName = statement.substring(statement.indexOf(SqlFragment.ADD_CONSTRAINT) + SqlFragment.ADD_CONSTRAINT.length(),
                    statement.indexOf(SqlFragment.FOREIGN_KEY)).trim();
            String columnName = statement.substring(statement.indexOf("(") + 1, statement.indexOf(")")).trim();
            String foreignTableName = statement.substring(
                    statement.indexOf(SqlFragment.REFERENCES) + SqlFragment.REFERENCES.size(), statement.length()).trim();
            ForeignKey foreignKey = new ForeignKey();
            foreignKey.name = foreignKeyName.toUpperCase();
            foreignKey.tableName = tableName;
            foreignKey.columnName = columnName;
            foreignKey.foreignTableName = foreignTableName;

            Table sqlTable = sqlTableMap.get(tableName);
            if (sqlTable == null) {
                sqlTable = new Table();
                sqlTable.name = tableName;
                sqlTableMap.put(tableName, sqlTable);
            }
            sqlTable.addForeign(foreignKey);

        } else if (statement.contains(SqlFragment.ADD_COLUMN)) {
            // ALTER TABLE AC_USER ADD COLUMN DEFTIMEZON VARGRAPHIC(50) CCSID 1200;
            def addColumnStartIndex = statement.indexOf(SqlFragment.ADD_COLUMN) + SqlFragment.ADD_COLUMN.length()
            String columnName = statement.substring(addColumnStartIndex, statement.indexOf(SqlFragment.SPACE, addColumnStartIndex + 1)).trim();
            TYPE columnType = getColumnType(statement);
            String columnSize = null;
            if (statement.contains("[")) {
                columnSize = statement.substring(statement.indexOf("[") + 1, statement.indexOf("]")).trim().replaceAll(" ", "").replaceAll(",", ".");
            }
            Column _column = new Column();
            _column.name = columnName;
            _column.type = columnType;
            _column.size = columnSize != null ? columnSize.replace(".0", "") : null;
            if (statement.contains(SqlFragment.UNIQUE)) {
                _column.isUnique = true;
            }
            addUpdateTableColumn(sqlTableMap, tableName, _column);
        } else if (statement.contains(SqlFragment.SET_DATA_TYPE)) {
            // ALTER TABLE AC_USER ALTER COLUMN PASSWORD SET DATA TYPE VARGRAPHIC (128) CCSID 1200;
            String columnName = statement.substring(statement.indexOf(SqlFragment.ALTER_COLUMN) + SqlFragment.ALTER_COLUMN.length(),
                    statement.indexOf(SqlFragment.SET_DATA_TYPE)).trim();
            TYPE columnType = getColumnType(statement);
            String columnSize = statement.substring(statement.indexOf("[") + 1, statement.indexOf("]")).trim().replaceAll(" ", "").replaceAll(",", ".");
            Column _column = new Column();
            _column.name = columnName;
            _column.type = columnType;
            _column.size = columnSize != null ? columnSize.replace(".0", "") : null;
            if (statement.contains(SqlFragment.UNIQUE)) {
                _column.isUnique = true;
            }
            addUpdateTableColumn(sqlTableMap, tableName, _column);
        } else if (statement.contains(SqlFragment.DROP_COLUMN)) {
            // ALTER TABLE AC_USER DROP COLUMN FULLNAME;
            String columnName = statement.substring(statement.indexOf(SqlFragment.DROP_COLUMN) + SqlFragment.DROP_COLUMN.length(),
                    statement.size()).trim();
            Table table = sqlTableMap.get(tableName);
            if (table != null) {
                table.removeColumnByName(columnName);
            }
        } else if (statement.contains(SqlFragment.UNIQUE)) {
            // ALTER TABLE CFS_FX_ORDER ADD CONSTRAINT UQ_CFS_FX_ORDER_FXNMBR_APPINSTANCE UNIQUE(FXNMBR,APPINSTANCE);
            String uniqueKeyName = statement.substring(statement.indexOf(SqlFragment.ADD_CONSTRAINT) + SqlFragment.ADD_CONSTRAINT.length(),
                    statement.indexOf(SqlFragment.UNIQUE)).trim();
            String columns = statement.substring(statement.indexOf("<") + 1, statement.indexOf(">")).trim().replaceAll(" ", "");
            UniqueConstraint uniqueKey = new UniqueConstraint();
            uniqueKey.name = uniqueKeyName;
            uniqueKey.addColumns(columns);
            Table sqlTable = sqlTableMap.get(tableName);
            if (sqlTable == null) {
                sqlTable = new Table();
                sqlTable.name = tableName;
                sqlTableMap.put(tableName, sqlTable);
            }
            sqlTable.addUniqueConstraints(uniqueKey);
            sqlTable.updateColumnsUniqueFlags();
        }
    } else {
        logger.warn "SQL Statement ignored: ($statement)"
    }

    return sqlTableMap.get(tableName);
}


def Table processHBM(String hbm, Map<String, Table> hbmTableMap) {

    hbm = hbm.substring(hbm.indexOf("<hibernate-mapping>"), hbm.size());

    mapping = new XmlSlurper().parseText(hbm)
    Table hbmTable = new Table();
    hbmTable.name = "${mapping.class.@table}".toString().toUpperCase().trim();

    if (Params.TABLES_TO_IGNORE.contains(hbmTable.name)
            || Params.SYS_TABLES.contains(hbmTable.name)) {
        return null;
    }

    hbmTable.className = "${mapping.class.@name}".toString().trim();
    mapping.class.id.each {
        Column column = new Column();
        hbmTable.addColumn(column);
        column.name = "${it.@column}".toString().toUpperCase();
        column.type = TYPE.BIGINT;
        column.size = "19";
        column.nullable = false;
        column.isId = true;
        column.isUnique = true;
    }
    mapping.class.property.each {
        Column column = new Column();
        column.nullable = true;
        column.isId = false;
        column.isUnique = false;
        hbmTable.addColumn(column);
        if (!Utils.isNullOrEmpty(it.column.@name)) {
            column.name = "${it.column.@name}".toString().toUpperCase();
        }
        if (!Utils.isNullOrEmpty(it.@type)) {
            String type = "${it.@type}".toString().toUpperCase()
            if (type.equals("TIMESTAMP")) {
                column.type = TYPE.TIMESTAMP;
            } else if (type.equals("JAVA.LANG.STRING")) {
                column.type = TYPE.VARGRAPHIC;
            } else if (type.equals("STRING")) {
                column.type = TYPE.VARGRAPHIC;
            } else if (type.equals("JAVA.LANG.INTEGER")) {
                column.type = TYPE.DECIMAL;
            } else if (type.equals("JAVA.LANG.DOUBLE")) {
                column.type = TYPE.DECIMAL;
            } else if (type.equals("JAVA.LANG.BOOLEAN")) {
                column.type = TYPE.SMALLINT;
                column.size = "5";
            } else if (type.equals("BOOLEAN")) {
                column.type = TYPE.SMALLINT;
                column.size = "5";
            } else if (type.equals("JAVA.LANG.SHORT")) {
                column.type = TYPE.SMALLINT;
                column.size = "5";
            } else if (type.equals("JAVA.LANG.LONG")) {
                column.type = TYPE.BIGINT;
                column.size = "19";
            } else if (type.equals("LONG")) {
                column.type = TYPE.BIGINT;
                column.size = "19";
            } else if (type.equals("DATE")) {
                column.type = TYPE.DATE;
            } else if (type.equals("TIME")) {
                column.type = TYPE.TIME;
            } else if (type.equals("TEXT")) {
                column.type = TYPE.CLOB;
            } else if (type.equals("BLOB")) {
                column.type = TYPE.BLOB;
            } else {
                throw new MojoExecutionException("The type '${it.@type}' could not be recognized")
            }
        } else if (!Utils.isNullOrEmpty(it.type.@name)) {
            it.type.param.each { param ->
                if (param.@name == 'type') {
                    def typeId = param.toString().trim();
                    if (typeId.equals("12")) {
                        column.type = TYPE.VARGRAPHIC;
                    } else if (typeId.equals("4")) {
                        column.type = TYPE.INTEGER;
                    }
                }
            }
        }
        if (!Utils.isNullOrEmpty(it.column.@length)) {
            column.size = "${it.column.@length}".toString();
        }
        if (!Utils.isNullOrEmpty(it.column.@"not-null")) {
            column.nullable = "${it.column.@"not-null"}".trim().equalsIgnoreCase("true") ? false : true;
        }
        // this would overwrite the previous values
        if (!Utils.isNullOrEmpty(it.column.@"sql-type")) {
            // currently just "decimal" sql-type is used
            String sqlType = "${it.column.@"sql-type"}".toString().toUpperCase();
            column.type = TYPE.DECIMAL;
            if (sqlType.contains("(")) {
                String columnSize = sqlType.substring(sqlType.indexOf("(") + 1, sqlType.indexOf(")"));
                column.size = columnSize.replaceAll(",", ".").replaceAll(" ", "").replace(".0", "");
            }
        }

        if (!Utils.isNullOrEmpty(it.column.@unique)) {
            column.isUnique = true;
        }

        def uniqueKeyName = "${it.column.@"unique-key"}".toUpperCase().trim();
        if (!Utils.isNullOrEmpty(uniqueKeyName)) {
            UniqueConstraint uniqueConstraint = hbmTable.getUniqueKeyByName(uniqueKeyName);
            if (uniqueConstraint == null) {
                uniqueConstraint = new UniqueConstraint();
                uniqueConstraint.name = uniqueKeyName;
                hbmTable.addUniqueConstraints(uniqueConstraint);
            }
            uniqueConstraint.addColumns(column.name);
            column.isUnique = true;
        }
    }

    mapping.class."many-to-one".each {
        Column column = new Column();
        hbmTable.addColumn(column);
        column.nullable = true;
        column.isId = false;
        column.isUnique = false;
        column.name = "${it.@column}".toString().toUpperCase();
        column.type = TYPE.BIGINT;
        if (!Utils.isNullOrEmpty(it.@"not-null")) {
            column.nullable = "${it.@"not-null"}".trim().equalsIgnoreCase("true") ? false : true;
        }
        ForeignKey foreignKey = new ForeignKey();
        hbmTable.addForeign(foreignKey);
        foreignKey.name = "${it.@"foreign-key"}".toString().toUpperCase().trim();
        foreignKey.tableName = hbmTable.name;
        foreignKey.columnName = "${it.@column}".toString();
        foreignKey.foreignClassName = "${it.@class}".toString().trim();
    }

    hbmTable.updateColumnsUniqueFlags();

    hbmTableMap.put(hbmTable.name, hbmTable);

    return hbmTable;
}

//////////////////////////////////////////////////////////////////////////////
// Comparison


def Map<String, Set<String>> getAllTableColumns(Map<SOURCE, Map<String, Table>> sourceTableMaps) {
    Map<String, Set<String>> allTableColumnsMap = new TreeMap<>();
    sourceTableMaps.each { source, tableMap ->
        tableMap.each { tableName, table ->
            Set<String> allColumns = allTableColumnsMap.get(tableName);
            if (allColumns == null) {
                allColumns = new TreeSet<>();
                allTableColumnsMap.put(tableName, allColumns);
            }
            table.columns.each {
                allColumns.add(it.name);
            }
        }
    }
    allTableColumnsMap
}


def String getColumnValue(Column column, XLS_COLUMN xlsColumn) {
    if (column == null) {
        return " ";
    }
    if (xlsColumn == XLS_COLUMN.NAME) {
        return column.name;
    } else if (xlsColumn == XLS_COLUMN.TYPE) {
        return column.type;
    } else if (xlsColumn == XLS_COLUMN.SIZE) {
        return column.size;
    } else if (xlsColumn == XLS_COLUMN.NULLABLE) {
        return column.nullable;
    } else if (xlsColumn == XLS_COLUMN.UNIQUE) {
        return column.isUnique;
    } else if (xlsColumn == XLS_COLUMN.PRIMARY_KEY) {
        return column.isId;
    } else {
        throw new MojoExecutionException("The requested column is not supported yet: $xlsColumn");
    }
}


def Integer compareColumns(List<Column> columns, XLS_COLUMN xlsColumn) {
    Set columnValues = new HashSet();

    columns.each { column ->
        if (column == null) {
            return xlsColumn.columnFactor;
        }
        if (xlsColumn == XLS_COLUMN.NAME) {
            if (column.name != null) {
                columnValues.add(column.name.toUpperCase().trim());
            }
        } else if (xlsColumn == XLS_COLUMN.TYPE) {
            if (column.type != null) {
                columnValues.add(column.type);
            }
        } else if (xlsColumn == XLS_COLUMN.SIZE) {
            if (column.size != null) {
                columnValues.add(column.size.toUpperCase().trim());
            }
        } else if (xlsColumn == XLS_COLUMN.NULLABLE) {
            if (column.nullable != null) {
                columnValues.add(column.nullable);
            }
        } else if (xlsColumn == XLS_COLUMN.UNIQUE) {
            if (column.isUnique != null) {
                columnValues.add(column.isUnique);
            }
        } else if (xlsColumn == XLS_COLUMN.PRIMARY_KEY) {
            if (column.isId != null) {
                columnValues.add(column.isId);
            }
        }
    }

    if (columnValues.size() < 2) {
        null;
    } else {
        xlsColumn.columnFactor;
    };
}


def Set areAllColumnsMatch(List<Column> columns) {
    def columnIndexes = new HashSet();
    XLS_COLUMN.values().each { xlsColumn ->
        def columnIndexHavingProblem = compareColumns(columns, xlsColumn)
        if (columnIndexHavingProblem != null) {
            columnIndexes.add(columnIndexHavingProblem)
        }
    }
    columnIndexes;
}

def boolean anyForeignKeyMissing(Map<SOURCE, ForeignKey> sources) {
    return Params.SOURCES.any { sourceName ->
        !sources.containsKey(SOURCE.byName(sourceName))
    }
}

def boolean areForeignKeyNamesMatch(Map<SOURCE, ForeignKey> sources) {
    Set<String> foreignKeyNames = new HashSet<>();
    Params.SOURCES.each { sourceName ->
        def source = SOURCE.byName(sourceName)
        ForeignKey foreignKey = sources.get(source);
        if (foreignKey != null && foreignKey.name != null) {
            foreignKeyNames.add(foreignKey.name)
        }
    }
    return foreignKeyNames.size() < 2;
}

def boolean anyUniqueConstraintMissing(Map<SOURCE, UniqueConstraint> sources) {
    return Params.SOURCES.any { sourceName ->
        !sources.containsKey(SOURCE.byName(sourceName))
    }
}

def boolean areUniqueConstraintNamesMatch(Map<SOURCE, UniqueConstraint> sources) {
    Set<String> uniqueConstraintNames = new HashSet<>();
    Params.SOURCES.each { sourceName ->
        def source = SOURCE.byName(sourceName)
        UniqueConstraint uniqueConstraint = sources.get(source);
        if (uniqueConstraint != null && uniqueConstraint.name != null) {
            uniqueConstraintNames.add(uniqueConstraint.name)
        }
    }
    return uniqueConstraintNames.size() < 2;
}


def logNValidateColumn(boolean columnMismatch, boolean columnMissing, String tableName, String columnName, List rowValues) {

    if(!columnMismatch && !columnMissing) {
        return;
    }

    def logTypes = new ArrayList();
    if((Params.CONSIDER_COLUMN_MISMATCH && columnMismatch)) {
        Params.anyErrorOccured = true;
        logTypes.add("mismatch")
    }
    if((Params.CONSIDER_COLUMN_MISSING && columnMissing)) {
        Params.anyErrorOccured = true;
        logTypes.add("missing")
    }

    if (logTypes.size() > 0) {
        if (Params.LOG_DETAILS) {
            PrintTable printTable = new PrintTable(Params.SOURCES.size() + 1, 6);
            int columnIndex = 1;
            int rowIndex = 1;
            printTable.add(rowIndex, columnIndex++, "Source")
            XLS_COLUMN.values().findAll { it.columnFactor != 0 }.each { headerColumn ->
                printTable.add(rowIndex, columnIndex++, headerColumn.toString())
            }
            int length = 5;
            def factor = 0;
            Params.SOURCES.each { source ->
                rowIndex++;
                columnIndex = 1
                def offset = 2 + factor++ * length;
                printTable.add(rowIndex, columnIndex++, source)
                rowValues.subList(offset, offset + length).each { columnValue ->
                    printTable.add(rowIndex, columnIndex++, columnValue)
                }
            }
            logger.info "${Params.errorNumber++}. Column ${logTypes.join("/")} on ${tableName}.${columnName}: \n${printTable.asString(5)}"
        } else {
            logger.info "${Params.errorNumber++}. Column ${logTypes.join("/")} on ${tableName}.${columnName}"
        }
    }

    if (Params.anyErrorOccured && Params.FAIL_IMMEDIATE) {
        throw new MojoFailureException("Build has been stopped due to an error!");
    }
}


def logNValidateForeignKey(boolean foreignKeyMissing, boolean  foreignKeyNamesMisMatch,
                           String tableName, Map<SOURCE, ForeignKey>  foreignKeySources) {

    if(!foreignKeyMissing && !foreignKeyNamesMisMatch) {
        return;
    }

    def logTypes = new ArrayList();
    if((Params.CONSIDER_CONSTRAINTS_MISSING && foreignKeyMissing)) {
        Params.anyErrorOccured = true;
        logTypes.add("missing")
    }
    if((Params.CONSIDER_CONSTRAINTS_NAME_MISMATCH && foreignKeyNamesMisMatch)) {
        Params.anyErrorOccured = true;
        logTypes.add("name-mismatch")
    }

    if (logTypes.size() > 0) {
        if (Params.LOG_DETAILS) {
            PrintTable printTable = new PrintTable(Params.SOURCES.size() + 1, 3);

            int columnIndex = 1;
            int rowIndex = 1;
            printTable.add(rowIndex, columnIndex++, "Source")
            printTable.add(rowIndex, columnIndex++, "Name")
            printTable.add(rowIndex, columnIndex++, "Value")

            Params.SOURCES.each { source ->
                ForeignKey foreignKey = foreignKeySources.get(SOURCE.byName(source));
                rowIndex++;
                columnIndex = 1
                printTable.add(rowIndex, columnIndex++, source)
                printTable.add(rowIndex, columnIndex++, foreignKey != null ? foreignKey.name : "")
                printTable.add(rowIndex, columnIndex++, foreignKey != null ? foreignKey.toString() : "")
            }
            logger.info "${Params.errorNumber++}. Foreign-Key ${logTypes.join("/")} on ${tableName}: \n${printTable.asString(5)}"
        } else {
            logger.info "${Params.errorNumber++}. Foreign-Key ${logTypes.join("/")} on ${tableName}"
        }
    }

    if (Params.anyErrorOccured && Params.FAIL_IMMEDIATE) {
        throw new MojoFailureException("Build has been stopped due to an error!");
    }
}


def logNValidateUniqueConstraints(boolean uniqueConstraintMissing, boolean uniqueConstraintNamesMisMatch,
                                  String tableName, Map<SOURCE, UniqueConstraint> uniqueConstraintSources) {

    if(!uniqueConstraintMissing && !uniqueConstraintNamesMisMatch) {
        return;
    }

    def logTypes = new ArrayList();
    if((Params.CONSIDER_CONSTRAINTS_MISSING && uniqueConstraintMissing)) {
        Params.anyErrorOccured = true;
        logTypes.add("missing")
    }
    if((Params.CONSIDER_CONSTRAINTS_NAME_MISMATCH && uniqueConstraintNamesMisMatch)) {
        Params.anyErrorOccured = true;
        logTypes.add("name-mismatch")
    }

    if (logTypes.size() > 0) {
        if (Params.LOG_DETAILS) {
            PrintTable printTable = new PrintTable(Params.SOURCES.size() + 1, 3);
            int columnIndex = 1;
            int rowIndex = 1;
            printTable.add(rowIndex, columnIndex++, "Source")
            printTable.add(rowIndex, columnIndex++, "Name")
            printTable.add(rowIndex, columnIndex++, "Value")
            Params.SOURCES.each { source ->
                UniqueConstraint uniqueConstraint = uniqueConstraintSources.get(SOURCE.byName(source));
                rowIndex++;
                columnIndex = 1
                printTable.add(rowIndex, columnIndex++, source)
                printTable.add(rowIndex, columnIndex++, uniqueConstraint != null ? uniqueConstraint.name : "")
                printTable.add(rowIndex, columnIndex++, uniqueConstraint != null ? uniqueConstraint.toString() : "")
            }
            logger.info "${Params.errorNumber++}. Unique-Constraint ${logTypes.join("/")} on ${tableName}: \n${printTable.asString(5)}"
        } else {
            logger.info "${Params.errorNumber++}. Unique-Constraint ${logTypes.join("/")} on ${tableName}"
        }
    }

    if (Params.anyErrorOccured && Params.FAIL_IMMEDIATE) {
        throw new MojoFailureException("Build has been stopped due to an error!");
    }
}


def Workbook createExcelReport(Map<String, Set<String>> allTableColumns, Map<SOURCE, Map<String, Table>> sourceTableMaps) {

    Workbook workbook = new ExcelFile().workbook {

        HSSFFont titleFont1 = workbook.createFont();
        titleFont1.setBoldweight(Font.BOLDWEIGHT_BOLD);
        titleFont1.setFontHeightInPoints((short) 14);

        HSSFFont titleFont2 = workbook.createFont();
        titleFont2.setBoldweight(Font.BOLDWEIGHT_BOLD);
        titleFont2.setFontHeightInPoints((short) 10);

        HSSFFont redFont = workbook.createFont();
        redFont.setColor(HSSFColor.RED.index);

        HSSFFont redBoldFont = workbook.createFont();
        redBoldFont.setColor(HSSFColor.RED.index);
        redBoldFont.setBoldweight(Font.BOLDWEIGHT_BOLD);

        def errorneousRowColumns = new TreeSet<String>();
        def errorneousLineNumbers = new HashSet<>();
        def columnsLastRowNumber = 1;

        def constraintsLastRowNumber = 1;
        def constraintsFirstTitles = Arrays.asList("Subsystem", "Table", "Type", "Missing Column", "Name Mismatch");

        // section for workbook formatting styles
        styles {
            font("bold") { Font font ->
                font.setBoldweight(Font.BOLDWEIGHT_BOLD)
            }
            font("redBoldFont") { Font font ->
                font.setColor(HSSFColor.RED.index);
                font.setBoldweight(Font.BOLDWEIGHT_BOLD);
            }
            cellStyle("header1") { CellStyle cellStyle ->
                cellStyle.setAlignment(CellStyle.ALIGN_CENTER);
                cellStyle.setFont(titleFont1);
            }
            cellStyle("header2") { CellStyle cellStyle ->
                cellStyle.setFont(titleFont2);
            }
            cellStyle("LIGHT_GREEN") { CellStyle style ->
                style.setFillForegroundColor(HSSFColor.LIGHT_GREEN.index);
                style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
            }
            cellStyle("LIGHT_TURQUOISE") { CellStyle style ->
                style.setFillForegroundColor(HSSFColor.LIGHT_TURQUOISE.index);
                style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
            }
            cellStyle("LIGHT_ORANGE") { CellStyle style ->
                style.setFillForegroundColor(HSSFColor.LIGHT_ORANGE.index);
                style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
            }
            cellStyle("LIGHT_CORNFLOWER_BLUE") { CellStyle style ->
                style.setFillForegroundColor(HSSFColor.LIGHT_CORNFLOWER_BLUE.index);
                style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
            }
            cellStyle("error") { CellStyle style ->
                style.setFillForegroundColor(HSSFColor.LIGHT_YELLOW.index);
                style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
            }
            cellStyle("errorColumn") { CellStyle style ->
                style.setFont(redBoldFont);
                style.setFillForegroundColor(HSSFColor.LIGHT_YELLOW.index);
                style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
                style.setBorderBottom(HSSFCellStyle.BORDER_MEDIUM);
                style.setBorderTop(HSSFCellStyle.BORDER_MEDIUM);
                style.setBorderRight(HSSFCellStyle.BORDER_MEDIUM);
                style.setBorderLeft(HSSFCellStyle.BORDER_MEDIUM);
            }
        }

        // section for workbook data
        data {
            [
                    sheet("Columns", 0, 3) {

                        def headerRow1 = new ArrayList();
                        headerRow1.add(" ");
                        headerRow1.add(" ");
                        Params.SOURCES.each { source ->
                            headerRow1.add(source);
                            XLS_COLUMN.values().each { column ->
                                if (column != XLS_COLUMN.NAME && column != XLS_COLUMN.TYPE) {
                                    headerRow1.add(" ")
                                }
                            }
                        }

                        def headerRow2 = new ArrayList();
                        headerRow2.add("Table");
                        headerRow2.add("Name");
                        Params.SOURCES.each {
                            XLS_COLUMN.values().each { column ->
                                if (column != XLS_COLUMN.NAME) {
                                    headerRow2.add("${column.toString()}")
                                }
                            }
                        }
                        headerRow2.add("Match");
                        headerRow2.add("Column Missing");
                        headerRow2.add("Subsystem");
                        header(headerRow1.findAll { "$it" });
                        header(headerRow2.findAll { "$it" });
                        row([" "]);

                        allTableColumns.each { tableName, columnNames ->

                            columnNames.each { columnName ->
                                def columnAttributes = new ArrayList();
                                columnAttributes.add(tableName);
                                columnAttributes.add(columnName);

                                def columnsForComparison = new ArrayList();

                                boolean columnMissing = false;
                                Params.SOURCES.each { sourceName ->
                                    def tableMap = sourceTableMaps.get(SOURCE.byName(sourceName));
                                    Column sourceTableColumn = null;
                                    if (tableMap != null) {
                                        Table sourceTable = tableMap.get(tableName);
                                        if (sourceTable != null) {
                                            sourceTableColumnMap = sourceTable.getColumnsAsMap();
                                            sourceTableColumn = sourceTableColumnMap.get(columnName);
                                        }
                                    }
                                    if (sourceTableColumn == null) {
                                        columnMissing = true;
                                    }
                                    columnAttributes.add(getColumnValue(sourceTableColumn, XLS_COLUMN.TYPE));
                                    columnAttributes.add(getColumnValue(sourceTableColumn, XLS_COLUMN.SIZE));
                                    columnAttributes.add(getColumnValue(sourceTableColumn, XLS_COLUMN.NULLABLE));
                                    columnAttributes.add(getColumnValue(sourceTableColumn, XLS_COLUMN.UNIQUE));
                                    columnAttributes.add(getColumnValue(sourceTableColumn, XLS_COLUMN.PRIMARY_KEY));

                                    columnsForComparison.add(sourceTableColumn);
                                }

                                def errorneousColumnFactors = areAllColumnsMatch(columnsForComparison);
                                columnAttributes.add(errorneousColumnFactors.size() == 0 ? "YES" : "NO");

                                columnAttributes.add(columnMissing ? "YES" : "NO");
                                if (tableName.contains("_")) {
                                    columnAttributes.add(tableName.substring(0, tableName.indexOf("_")));
                                } else {
                                    columnAttributes.add(" ");
                                }

                                errorneousColumnFactors.each { columnFactor ->
                                    Params.SOURCES.each { sourceName ->
                                        def columnIndex = 2 + (Params.SOURCES.indexOf(sourceName) * (XLS_COLUMN.values().size() - 1)) + columnFactor;
                                        errorneousRowColumns.add("${(getRowCount() + 1)}__$columnIndex");
                                        errorneousLineNumbers.add(getRowCount() + 1);
                                    }
                                }

                                def details = columnAttributes.findAll { "$it " }

                                logNValidateColumn(errorneousColumnFactors.size() > 0, columnMissing, tableName, columnName, details);

                                row(details);
                            }
                            row([" "]);
                            row([" "]);
                        }
                        columnsLastRowNumber = getRowCount() + 1;
                    },

                    sheet("Constraints", 0, 3) {

                        def headerRow1 = new ArrayList();
                        constraintsFirstTitles.each {
                            headerRow1.add(" ");
                        }
                        Params.SOURCES.each { source ->
                            headerRow1.add(source);
                            headerRow1.add(" ");
                        }

                        def headerRow2 = new ArrayList();
                        constraintsFirstTitles.each {
                            headerRow2.add(it);
                        }
                        Params.SOURCES.each {
                            headerRow2.add("Name");
                            headerRow2.add("Value");
                        }
                        header(headerRow1.findAll { "$it" });
                        header(headerRow2.findAll { "$it" });
                        row([" "]);

                        // Preparing the data
                        def foreignKeyToSources = new HashMap<String, Map<SOURCE, ForeignKey>>();
                        def tableNameToforeignKeys = new HashMap<String, Set<String>>();
                        def uniqueConstraintToSources = new HashMap<String, Map<SOURCE, UniqueConstraint>>();
                        def tableNameToUniqueConstraints = new HashMap<String, Set<String>>();

                        allTableColumns.keySet().each { tableName ->

                            Params.SOURCES.each { sourceName ->
                                SOURCE source = SOURCE.byName(sourceName)
                                def tableMap = sourceTableMaps.get(source);
                                Table sourceTable = null;
                                if (tableMap != null) {
                                    sourceTable = tableMap.get(tableName);
                                }
                                if (sourceTable != null) {

                                    sourceTable.foreignKeys.each { foreignKey ->
                                        // update the foreignKeyToSources map
                                        Map<SOURCE, ForeignKey> sourcesMap = foreignKeyToSources.get(foreignKey.toString());
                                        if (sourcesMap == null) {
                                            sourcesMap = new TreeMap<SOURCE, ForeignKey>();
                                            foreignKeyToSources.put(foreignKey.toString(), sourcesMap);
                                        }
                                        sourcesMap.put(source, foreignKey);
                                        // update the tableNameToforeignKeys map
                                        Set<String> foreignKeys = tableNameToforeignKeys.get(tableName);
                                        if (foreignKeys == null) {
                                            foreignKeys = new TreeSet<String>();
                                            tableNameToforeignKeys.put(tableName, foreignKeys);
                                        }
                                        foreignKeys.add(foreignKey.toString());
                                    }

                                    sourceTable.uniqueConstraints.each { uniqueConstraint ->
                                        // update the uniqueConstraintToSources map
                                        Map<SOURCE, UniqueConstraint> sourcesMap = uniqueConstraintToSources.get(uniqueConstraint.toString());
                                        if (sourcesMap == null) {
                                            sourcesMap = new TreeMap<SOURCE, UniqueConstraint>();
                                            uniqueConstraintToSources.put(uniqueConstraint.toString(), sourcesMap);
                                        }
                                        sourcesMap.put(source, uniqueConstraint);
                                        // update the tableNameToUniqueConstraints map
                                        Set<String> uniqueConstraints = tableNameToUniqueConstraints.get(tableName);
                                        if (uniqueConstraints == null) {
                                            uniqueConstraints = new TreeSet<String>();
                                            tableNameToUniqueConstraints.put(tableName, uniqueConstraints);
                                        }
                                        uniqueConstraints.add(uniqueConstraint.toString());
                                    }
                                }
                            }
                        }

                        // adding the columns
                        allTableColumns.keySet().each { tableName ->

                            Set<String> foreignKeys = tableNameToforeignKeys.get(tableName);
                            if (foreignKeys != null && !foreignKeys.isEmpty()) {
                                foreignKeys.each { foreignKeyValue ->
                                    def columnValues = new ArrayList();
                                    if (tableName.contains("_")) {
                                        columnValues.add(tableName.substring(0, tableName.indexOf("_")));
                                    } else {
                                        columnValues.add(" ");
                                    }
                                    columnValues.add(tableName);
                                    columnValues.add("Foreign Key");

                                    Map<SOURCE, ForeignKey> sources = foreignKeyToSources.get(foreignKeyValue);

                                    def foreignKeyMissing = anyForeignKeyMissing(sources);
                                    columnValues.add(foreignKeyMissing ? "YES" : "NO");

                                    def foreignKeyNamesMatch = areForeignKeyNamesMatch(sources);
                                    columnValues.add(foreignKeyNamesMatch ? "NO" : "YES");

                                    logNValidateForeignKey(foreignKeyMissing, !foreignKeyNamesMatch, tableName, sources);

                                    Params.SOURCES.each { sourceName ->
                                        SOURCE source = SOURCE.byName(sourceName);
                                        if (sources != null && sources.containsKey(source)) {
                                            columnValues.add(sources.get(source).name);
                                            columnValues.add(sources.get(source).toString());
                                        } else {
                                            columnValues.add(" ");
                                            columnValues.add(" ");
                                        }
                                    }
                                    row(columnValues.findAll { "$it " });
                                }
                            }

                            Set<String> uniqueConstraints = tableNameToUniqueConstraints.get(tableName);
                            if (uniqueConstraints != null && !uniqueConstraints.isEmpty()) {
                                uniqueConstraints.each { uniqueConstraintValue ->
                                    def columnValues = new ArrayList();
                                    if (tableName.contains("_")) {
                                        columnValues.add(tableName.substring(0, tableName.indexOf("_")));
                                    } else {
                                        columnValues.add(" ");
                                    }
                                    columnValues.add(tableName);
                                    columnValues.add("Unique Constraint");

                                    Map<SOURCE, UniqueConstraint> sources = uniqueConstraintToSources.get(uniqueConstraintValue);

                                    def uniqueConstraintMissing = anyUniqueConstraintMissing(sources);
                                    columnValues.add(uniqueConstraintMissing ? "YES" : "NO");

                                    def uniqueConstraintNamesMatch = areUniqueConstraintNamesMatch(sources);
                                    columnValues.add(uniqueConstraintNamesMatch ? "YES" : "NO");

                                    logNValidateUniqueConstraints(uniqueConstraintMissing, !uniqueConstraintNamesMatch, tableName, sources);

                                    Params.SOURCES.each { sourceName ->
                                        SOURCE source = SOURCE.byName(sourceName);
                                        if (sources != null && sources.containsKey(source)) {
                                            columnValues.add(sources.get(source).name);
                                            columnValues.add(sources.get(source).toString());
                                        } else {
                                            columnValues.add(" ");
                                            columnValues.add(" ");
                                        }
                                    }
                                    row(columnValues.findAll { "$it " });
                                }
                            }

                            if ((foreignKeys != null && !foreignKeys.isEmpty())
                                    || (uniqueConstraints != null && !uniqueConstraints.isEmpty())) {
                                row([" "]);
                            }
                        }
                        constraintsLastRowNumber = getRowCount() + 1;
                    },

                    sheet("Used Configuration", 1, 2) {

                        row(["Option", "Value", "Description"]);
                        row([" "]);
                        row(["Execution Time", new Date().format('dd.MM.yyyy HH:mm:ss')]);
                        row([" "]);
                        row(["Sources", Params.SOURCES.join(", "), "For these sources a report has been generated"]);
                        row([" "]);
                        row(["Project Root Folder", Params.PROJECT_ROOT_FOLDER, "The path to the root of the Samanta project"]);
                        row(["ERD Project File", Params.ERD_PROJECT_FILE_PATH, "The path to the ERD project file (.vpp)"]);
                        row(["Re-Export ERD to XML", Params.RE_EXPORT_ERD_XML, "This flag controls whether to convert the Visual Paradigm project vpp file to xml. This would be necessary for the first time or after updating the ERD diagram."]);
                        row(["Visual Paradigm Export Script", Params.Visual_PARADIGM_EXPORT_SCRIPT, "The path to the Visual Paradigm script for vpp to xml conversion. This script is normally placed in the VP installation folder."]);
                        row(["User directory", Params.EXECUTION_DIRECTORY, "The directory where the Visual Paradigm xml file will be read from. The report will also be put inside this folder. In case this folder is not set, the same folder as the script will be taken as the user folder."]);
                        row([" "]);
                        row(["Tables to ignore"]);
                        row(["System tables", Params.SYS_TABLES.join(",")]);
                        row(["Others", Params.TABLES_TO_IGNORE.join(",")]);
                        row([" "]);
                        row(["Database Information"]);
                        row(["Url", Params.database.url]);
                        row(["Schema", Params.database.schemaName]);
                        row(["Username", Params.database.user]);
                        row(["Password", Params.database.password]);

                    }
            ]
        }

        // section for applying commands i.e. apply a cell style, merge cells etc.
        commands {
            mergeCells(sheet: "Columns", rows: 1, columns: 3..7)
            applyCellStyle(sheet: "Columns", cellStyle: "LIGHT_GREEN", rows: 1..columnsLastRowNumber, columns: 3..7)
            mergeCells(sheet: "Columns", rows: 1, columns: 8..12)
            applyCellStyle(sheet: "Columns", cellStyle: "LIGHT_TURQUOISE", rows: 1..columnsLastRowNumber, columns: 8..12)
            mergeCells(sheet: "Columns", rows: 1, columns: 13..17)
            applyCellStyle(sheet: "Columns", cellStyle: "LIGHT_ORANGE", rows: 1..columnsLastRowNumber, columns: 13..17)
            mergeCells(sheet: "Columns", rows: 1, columns: 18..22)
            applyCellStyle(sheet: "Columns", cellStyle: "LIGHT_CORNFLOWER_BLUE", rows: 1..columnsLastRowNumber, columns: 18..22)
            errorneousLineNumbers.each { rowNumber ->
                applyCellStyle(sheet: "Columns", cellStyle: "error", rows: rowNumber, columns: 1..50)
            }
            errorneousRowColumns.each {
                def tableColumns = it.split("__");
                def row = Integer.valueOf(tableColumns[0]);
                def column = Integer.valueOf(tableColumns[1]);
                applyCellStyle(sheet: "Columns", cellStyle: "errorColumn", rows: row, columns: column);
            }
            applyCellStyle(sheet: "Columns", cellStyle: "header1", rows: 1, columns: 1..50)
            applyCellStyle(sheet: "Columns", cellStyle: "header2", rows: 2, columns: 1..50)

            mergeCells(sheet: "Constraints", rows: 1, columns: (constraintsFirstTitles.size() + 1)..(constraintsFirstTitles.size() + 2))
            applyCellStyle(sheet: "Constraints", cellStyle: "LIGHT_GREEN", rows: 1..constraintsLastRowNumber, columns: (constraintsFirstTitles.size() + 1)..(constraintsFirstTitles.size() + 2))
            mergeCells(sheet: "Constraints", rows: 1, columns: (constraintsFirstTitles.size() + 3)..(constraintsFirstTitles.size() + 4))
            applyCellStyle(sheet: "Constraints", cellStyle: "LIGHT_TURQUOISE", rows: 1..constraintsLastRowNumber, columns: (constraintsFirstTitles.size() + 3)..(constraintsFirstTitles.size() + 4))
            mergeCells(sheet: "Constraints", rows: 1, columns: (constraintsFirstTitles.size() + 5)..(constraintsFirstTitles.size() + 6))
            applyCellStyle(sheet: "Constraints", cellStyle: "LIGHT_ORANGE", rows: 1..constraintsLastRowNumber, columns: (constraintsFirstTitles.size() + 5)..(constraintsFirstTitles.size() + 6))
            mergeCells(sheet: "Constraints", rows: 1, columns: (constraintsFirstTitles.size() + 7)..(constraintsFirstTitles.size() + 8))
            applyCellStyle(sheet: "Constraints", cellStyle: "LIGHT_CORNFLOWER_BLUE", rows: 1..constraintsLastRowNumber, columns: (constraintsFirstTitles.size() + 7)..(constraintsFirstTitles.size() + 8))

            autSize(sheet: "Used Configuration", columns: 1..3);
            applyCellStyle(sheet: "Used Configuration", cellStyle: "header2", rows: 1, columns: 1..50);
            applyCellStyle(sheet: "Used Configuration", cellStyle: "header2", rows: 1..50, columns: 1);

        }
    }

    workbook
}


def void execute() {

    Map<SOURCE, Map<String, Table>> sourceTableMaps = new HashMap<>();

    // Gathering HBM information
    if (Params.SOURCES.contains(SOURCE.HBM.toString())) {
        def hbmTableMap = new HashMap<String, Table>()
        def hbmClassToTableName = new HashMap<String, String>()
        def hbmFolders = Arrays.asList(
                new File(Params.PROJECT_ROOT_FOLDER + "\\samantaObjects\\src\\main\\resources\\hibernate"),
                new File(Params.PROJECT_ROOT_FOLDER + "\\fas\\src\\main\\resources\\hibernate")
        )
        // create a map from table names to the content of the respective hbm files
        hbmFolders.each { folder ->
            folder.eachFileRecurse(FileType.FILES) { file ->
                if (file.path.endsWith(".xml")) {
                    def hbmContent = file.text;
                    Table hbmTable = processHBM(hbmContent, hbmTableMap);
                    if (hbmTable != null) {
                        hbmClassToTableName.put(hbmTable.className, hbmTable.name);
                    }
                }
            }
        }
        // populate the missing class-names
        hbmTableMap.each { tableName, table ->
            table.foreignKeys.each {
                it.foreignTableName = hbmClassToTableName.get(it.foreignClassName);
            }
        }
        sourceTableMaps.put(SOURCE.HBM, hbmTableMap);
    }

    // Gathering SQL information
    if (Params.SOURCES.contains(SOURCE.SQL.toString())) {
        def sqlTableMap = new HashMap<String, Table>();
        def sqlFiles = Arrays.asList(
                "DROP_CONSTRAINTS_BEFORE_3_0_0.sql",
                "MANUAL_PATCH_SQLS_3_0_0.sql",
                "CREATE_3_0_0.sql",
                "CONSTRAINTS_3_0_0.sql",
                //"INDEXES_3_0_0.sql",
                "AFTER_PATCH_SQLS_3_0_0.sql",
                "DROP_AFTER_3_0_0.sql",
                "MANUAL_PATCH_SQLS_AFTER_3_0_0.sql"
        )

        def sqlFolder = Params.PROJECT_ROOT_FOLDER + "\\briglObjects\\src\\main\\resources\\dbupgrade\\v3_0_0\\";
        sqlFiles.each {
            File file = new File(sqlFolder + it);
            String fileContent = file.text;
            Pattern commentPattern = Pattern.compile("(?:/\\*[^;]*?\\*/)|(?:--[^;]*?\$)", Pattern.DOTALL | Pattern.MULTILINE);
            fileContent = commentPattern.matcher(fileContent).replaceAll("");
            List<String> sqlStatements = Arrays.asList(fileContent.split(SqlFragment.SEPARATOR));
            sqlStatements.each {
                if (!it.trim().isEmpty()) {
                    Table sqlTable = processSQLStatement(it, sqlTableMap);
                }
            }
        }
        sourceTableMaps.put(SOURCE.SQL, sqlTableMap);
    }

    // Gathering ERD information
    if (Params.SOURCES.contains(SOURCE.ERD.toString())) {
        def erdTableMap = new HashMap<String, Table>();
        processErdProjectFile(erdTableMap, Params.RE_EXPORT_ERD_XML);
        sourceTableMaps.put(SOURCE.ERD, erdTableMap);
    }

    // Gathering DB information
    if (Params.SOURCES.contains(SOURCE.DB.toString())) {
        def dbTableMap = new HashMap<String, Table>();
        sourceTableMaps.put(SOURCE.DB, Params.database.tables);
    }

    Map<String, Set<String>> allTableColumns = getAllTableColumns(sourceTableMaps);

    def workbook = createExcelReport(allTableColumns, sourceTableMaps);

    def reportFilePath = "${Params.EXECUTION_DIRECTORY}${System.getProperty("file.separator")}comparison.xls"
    FileOutputStream fileOut = new FileOutputStream(reportFilePath);
    workbook.write(fileOut);
    fileOut.close();

    if(Params.anyErrorOccured) {
        throw new MojoFailureException("The build has been stopped due to some difference(s) between the following sources: $sources\n" +
                "For further details consult the report file at $reportFilePath");
    }
}

execute();


