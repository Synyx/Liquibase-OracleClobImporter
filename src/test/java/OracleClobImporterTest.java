import liquibase.database.Database;
import liquibase.database.DatabaseConnection;

import liquibase.database.jvm.JdbcConnection;

import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;

import liquibase.resource.ResourceAccessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.synyx.liquibase.oracle.OracleClobImporter;

import java.io.IOException;
import java.io.InputStream;


public class OracleClobImporterTest {

    private OracleClobImporter taskChange;

    private Database database;
    private DatabaseConnection databaseConnection;
    private ResourceAccessor resourceAccessor;

    @Before
    public void setUp() {

        taskChange = new OracleClobImporter();

        database = mock(Database.class);
        databaseConnection = mock(JdbcConnection.class);
        resourceAccessor = mock(ResourceAccessor.class);

        when(database.getConnection()).thenReturn(databaseConnection);
        taskChange.setFileOpener(resourceAccessor);
    }


    @Test
    public void testSetUpNullFileName() throws SetupException {

        taskChange.setFileName(null);

        taskChange.setUp();
    }


    @Test
    public void testSetUp() throws SetupException, IOException {

        when(resourceAccessor.getResourceAsStream("myFile")).thenReturn(new InputStream() {

                @Override
                public int read() throws IOException {

                    return -1;
                }
            });

        taskChange.setFileName("myFile");

        taskChange.setUp();
    }


    @Test
    public void testValidate() {

        taskChange.setColumnName("FooColumn");
        taskChange.setTableName("FooTable");

        ValidationErrors errors = taskChange.validate(database);
        assertTrue(errors.hasErrors());

        taskChange.setFileName("");
        errors = taskChange.validate(database);
        assertFalse(errors.hasErrors());
    }


    @Test
    public void testGetConfirmationMessage() throws IOException, SetupException {

        when(resourceAccessor.getResourceAsStream("myFile")).thenReturn(new InputStream() {

                @Override
                public int read() throws IOException {

                    return -1;
                }
            });

        taskChange.setFileName("myFile");
        taskChange.setColumnName("FooColumn");
        taskChange.setTableName("FooTable");
        taskChange.setUp();

        assertEquals("0 characters successfully written to FooTable.FooColumn", taskChange.getConfirmationMessage());
    }
}
