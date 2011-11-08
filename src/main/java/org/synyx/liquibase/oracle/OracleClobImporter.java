package org.synyx.liquibase.oracle;

import liquibase.change.custom.CustomTaskChange;

import liquibase.database.Database;

import liquibase.database.jvm.JdbcConnection;

import liquibase.exception.CustomChangeException;
import liquibase.exception.DatabaseException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;

import liquibase.resource.ResourceAccessor;

import org.apache.commons.io.IOUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class OracleClobImporter implements CustomTaskChange {

    private static final String PARAM_TABLE_NAME = "tableName";
    private static final String PARAM_COLUMN_NAME = "columnName";

    private static final String DEFAULT_ENCODING = System.getProperty("file.encoding", "UTF-8");
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    private ResourceAccessor resourceAccessor;

    private String tableName;
    private String columnName;
    private String fileName;
    private String fileEncoding = DEFAULT_ENCODING;
    private String clobContent;

    private String where;
    private int maxSize = DEFAULT_BUFFER_SIZE;

    public void setTableName(String tableName) {

        this.tableName = tableName;
    }


    public void setColumnName(String columnName) {

        this.columnName = columnName;
    }


    public void setFileName(String fileName) {

        this.fileName = fileName;
    }


    public void setFileEncoding(String fileEncoding) {

        this.fileEncoding = fileEncoding;
    }


    public void setClobContent(String clobContent) {

        this.clobContent = clobContent;
    }


    public void setWhere(String where) {

        this.where = where;
    }


    public void setMaxSize(int maxSize) {

        this.maxSize = maxSize;
    }


    public void execute(Database database) throws CustomChangeException {

        JdbcConnection jdbcConnection = (JdbcConnection) database.getConnection();

        emptyClob(jdbcConnection);
        fillClobWithContent(jdbcConnection);
    }


    private void fillClobWithContent(JdbcConnection jdbcConnection) throws CustomChangeException {

        Statement statement = null;
        ResultSet rs = null;

        try {
            statement = jdbcConnection.createStatement();
            rs = statement.executeQuery(getSelectQuery());

            while (rs.next()) {
                Clob clob = rs.getClob(columnName);
                Writer clobWriter = new BufferedWriter(clob.setCharacterStream(1), maxSize);
                clobWriter.write(clobContent);

                clobWriter.close();
            }
        } catch (SQLException e) {
            throw new CustomChangeException(e);
        } catch (IOException e) {
            throw new CustomChangeException(e);
        } catch (DatabaseException e) {
            throw new CustomChangeException(e);
        } finally {
            closeResultSet(rs);
            closeStatement(statement);
        }
    }


    private void closeResultSet(ResultSet rs) {

        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            // Swallow exception
        }
    }


    private void emptyClob(JdbcConnection jdbcConnection) throws CustomChangeException {

        Statement statement = null;

        try {
            statement = jdbcConnection.createStatement();
            statement.execute(getUpdateQuery());
        } catch (SQLException e) {
            throw new CustomChangeException(e);
        } catch (DatabaseException e) {
            throw new CustomChangeException(e);
        } finally {
            closeStatement(statement);
        }
    }


    private void closeStatement(Statement statement) {

        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            // Swallow exception
        }
    }


    private String getSelectQuery() {

        StringBuilder selectSQL = new StringBuilder("SELECT ").append(columnName).append(" FROM ").append(tableName);

        if (where != null && !where.isEmpty()) {
            selectSQL.append(" WHERE ").append(where);
        }

        selectSQL.append(" FOR UPDATE NOWAIT");

        return selectSQL.toString();
    }


    private String getUpdateQuery() {

        StringBuilder updateSQL = new StringBuilder("UPDATE ").append(tableName).append(" SET ").append(columnName)
            .append(" = empty_clob()");

        if (where != null && !where.isEmpty()) {
            updateSQL.append(" WHERE ").append(where);
        }

        return updateSQL.toString();
    }


    public void setUp() throws SetupException {

        if (fileName != null) {
            try {
                clobContent = IOUtils.toString(resourceAccessor.getResourceAsStream(fileName), fileEncoding);
            } catch (IOException e) {
                throw new SetupException(e);
            }
        }
    }


    public void setFileOpener(ResourceAccessor resourceAccessor) {

        this.resourceAccessor = resourceAccessor;
    }


    @Override
    public ValidationErrors validate(Database database) {

        ValidationErrors validationErrors = new ValidationErrors();

        if (!(database.getConnection() instanceof JdbcConnection)) {
            validationErrors.addError("This custom change only works with JDBC connections");
        }

        if (fileName == null && clobContent == null) {
            validationErrors.addError("Either fileName or clobContent must be provided");
        }

        validationErrors.checkRequiredField(PARAM_TABLE_NAME, tableName);
        validationErrors.checkRequiredField(PARAM_COLUMN_NAME, columnName);

        return null;
    }


    @Override
    public String getConfirmationMessage() {

        return String.format("%d characters successfully written to %s.%s", clobContent.length(), tableName,
                columnName);
    }
}
