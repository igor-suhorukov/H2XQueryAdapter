package org.basex;

import com.github.database.rider.core.api.connection.ConnectionHolder;
import com.github.database.rider.core.api.dataset.ExpectedDataSet;
import com.github.database.rider.junit5.DBUnitExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@ExtendWith(DBUnitExtension.class)
public class H2XQueryAdapterTest {

    private Connection connection;

    private ConnectionHolder connectionHolder = () -> connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:testDb", "sa", "");
        connection.setAutoCommit(true);
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    @ExpectedDataSet("xresult.xml")
    void testXQueryTable() throws Exception{
        try (Statement statement = connection.createStatement()){
            String xqueryAlias = "create alias xquery for \"" + H2XQueryAdapter.class.getName() + ".xquery\"";
            statement.executeUpdate(xqueryAlias);

            statement.executeUpdate("create table xresult (GR VARCHAR(500) not null,AR varchar, VER VARCHAR(50)) as \n select * from xquery('declare variable $getHeader as xs:boolean external := false(); declare variable $getData as xs:boolean external := true(); \n" +
                    "<csv>\n" +
                    "{\n" +
                    "if($getHeader) then( \n" +
                    "    <record><mavengr>VARCHAR(500) not null</mavengr><artifactname>varchar</artifactname><versionname>VARCHAR(50)</versionname></record>\n" +
                    " ),\n" +
                    " if($getData) then((\n" +
                    "    for $row in doc(\"http://central.maven.org/maven2/org/springframework/spring-context/5.1.4.RELEASE/spring-context-5.1.4.RELEASE.pom\")//*:dependency\n" +
                    "    return \n" +
                    "    <record><mavengr>{$row/*:groupId/text()}</mavengr><artifactname>{$row/*:artifactId/text()}</artifactname><versionname>{$row/*:version/text()}</versionname></record>\n" +
                    " ))\n" +
                    "}\n" +
                    "</csv>')");
        }

    }
}
