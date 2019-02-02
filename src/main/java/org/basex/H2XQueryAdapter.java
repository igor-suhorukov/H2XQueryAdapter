package org.basex;

import org.basex.build.csv.CsvOptions;
import org.basex.core.Context;
import org.basex.io.serial.Serializer;
import org.basex.io.serial.SerializerMode;
import org.basex.io.serial.SerializerOptions;
import org.basex.io.serial.csv.H2JdbcSerializer;
import org.basex.query.QueryException;
import org.basex.query.QueryProcessor;
import org.basex.query.iter.BasicIter;
import org.basex.query.value.item.Item;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.tools.SimpleResultSet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class H2XQueryAdapter {

    private static final int KEY_INDEX = 1;
    private static final int VALUE_INDEX = 2;

    public static ResultSet xquery(Connection connection, String query) throws Exception{
        return xquery(connection, query, Collections.emptyMap(), true);
    }

    public static ResultSet xquery(Connection connection, String query, String queryParameters) throws Exception{
        Map<String, Object> parameters = queryParameters(connection, queryParameters);
        return xquery(connection, query, parameters, true);
    }

    public static ResultSet xquery(@SuppressWarnings("unused") Connection connection, String query,
                                   Map<String, Object> queryParameters, boolean containsHeaderType) throws Exception{

        try(QueryProcessor queryProcessor = new QueryProcessor(query, new Context())) {
            bindExternalParameters(queryProcessor, queryParameters);
            queryProcessor.bind("getHeader", containsHeaderType);
            queryProcessor.bind("getData", isDataRequest(connection));
            BasicIter<Item> queryIterator = queryProcessor.value().iter();
            for(Item item; (item = queryIterator.next()) != null;) {
                SerializerOptions opts = SerializerMode.DEFAULT.get();
                CsvOptions csvOptions = opts.get(SerializerOptions.CSV);
                csvOptions.assign("header","true");
                SimpleResultSet resultSet = new SimpleResultSet();
                Session session = (Session) ((JdbcConnection) connection).getSession();
                try(Serializer serializer = new H2JdbcSerializer(resultSet, opts, containsHeaderType, session)){
                    serializer.serialize(item);
                }
                return resultSet;
            }
        }
        throw new IllegalArgumentException("Empty xQuery execution result");
    }

    private static Map<String, Object> queryParameters(Connection connection, String parameters) {
        try {
            if(parameters!=null && !parameters.isEmpty()) {
                try (Statement parameterStatement = connection.createStatement()) {
                    try (ResultSet paramSet = parameterStatement.executeQuery(parameters)) {
                        Map<String, Object> parameterMap = new HashMap<>();
                        while (paramSet.next()) {
                            parameterMap.put(paramSet.getString(KEY_INDEX), paramSet.getObject(VALUE_INDEX));
                        }
                        return parameterMap;
                    }
                }
            }
            return Collections.emptyMap();
        } catch (Exception ex) {
            throw new IllegalArgumentException("Exception occured on query parameter bindings."+ ex.getMessage(), ex);
        }
    }

    private static void bindExternalParameters(QueryProcessor queryProcessor,
                                               Map<String, Object> queryParameters) throws QueryException {
        if(queryParameters!=null) {
            for (Map.Entry<String, Object> entry : queryParameters.entrySet()) {
                queryProcessor.bind(entry.getKey(), entry.getValue());
            }
        }
    }

    private static boolean isDataRequest(Connection connection) throws SQLException {
        return !connection.getMetaData().getURL().startsWith("jdbc:columnlist:");
    }

}
