import groovy.sql.Sql
import com.branegy.dbmaster.model.*
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JDBCDialect

def test_server    = p_database.split("\\.")[0]
def test_database  = p_database.split("\\.")[1]

connectionSrv = dbm.getService(ConnectionService.class)

RevEngineeringOptions options = new RevEngineeringOptions()
options.database = test_database
options.importViews = false
options.importTables = true

connectionInfo = connectionSrv.findByName(test_server)
connector = ConnectionProvider.getConnector(connectionInfo)

logger.info("Connecting to ${test_server}")

dialect = connector.connect()

logger.info("Loading list of tables from database ${test_database}")

model = dialect.getModel(test_server, options)

connection = connector.getJdbcConnection(test_database)
dbm.closeResourceOnExit(connection)

def sql = new Sql(connection)

println """<table cellspacing="0" class="simple-table" border="1">
               <tr style=\"background-color:#EEE\">
                   <td>Table</td>
                   <td>Rows</td>
                   <td>Execution Time (sec)</td>
               </tr>"""


def benchmark = { closure ->
  start = System.currentTimeMillis()
  closure.call()
  now = System.currentTimeMillis()
  now - start
}

model.tables.each { table ->
    logger.info("Gettting counts for table ${table.name}")

    query = generateSql(table, dialect)
    try {
        logger.info("Query = ${query}")
        def rowsInTable
        // return
        def duration = benchmark {
            def firstRow = sql.rows(query.toString())[0]
            rowsInTable = firstRow==null ? -1 : firstRow.getAt(0)
        }

        println """<tr>
                    <td>${table.name}</td>
                    <td align=\"right\">${rowsInTable}</td>
                    <td align=\"right\">${duration/1000.0}</td>
                   </tr>"""
    } catch (Exception e) {
        e.printStackTrace()
        println """<tr>
                    <td><b>${table.name}</b></td>
                    <td align=\"right\">---</td>
                    <td align=\"right\">---</td></tr>"""
        logger.error("Table ${table.name} - ${e.getMessage()}")
    }
}

connection.close()
println "</table>"


def generateSql(Table table, JDBCDialect dialect) {
    def tableName = table.name
    def max_rows = 1

    switch (dialect.getDialectName().toLowerCase()) {
    case "oracle":
        return  "select count(*) from \"${tableName}\"" // where ROWNUM <= ${max_rows}
    case "sqlserver":
        return  "select count(*) from [${table.schema}].[${table.simpleName}] with (NOLOCK)" // top ${max_rows} *
    case "mysql":
        return  "select count(*) from ${tableName}" // limit 0,${max_rows}
    default:
        return  "select count(*) from ${tableName}"
    }
}