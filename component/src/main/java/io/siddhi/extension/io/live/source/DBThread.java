//package io.siddhi.extension.io.live.source;
//
//import com.c8db.C8Cursor;
//import com.c8db.C8DB;
//import com.c8db.entity.BaseDocument;
//import com.google.gson.Gson;
//import io.siddhi.core.stream.input.source.SourceEventListener;
//import io.siddhi.extension.io.live.source.Thread.AbstractThread;
//import io.siddhi.extension.io.live.utils.Monitor;
////import net.minidev.json.JSONObject;
//import lombok.Builder;
//import org.apache.tapestry5.json.JSONObject;
//
//@Builder
//public class DBThread extends AbstractThread {
//    private final SourceEventListener sourceEventListener;
//    private String hostName;
//    private int port = 443;
//    private String apiKey;
//    private String user = "root";
//    private String selectSQL;
//
//    @Override
//    public void run() {
//        final C8DB c8db = new C8DB.Builder().useSsl(true).host(hostName , port).apiKey(apiKey).user(user).build();
//        final C8Cursor<BaseDocument> cursor = c8db.db(null , "_system")
//                .query(selectSQL, null, null, BaseDocument.class);
//
//        while (cursor.hasNext() && isThreadRunning) {
//            if(isPaused) {
//                LOGGER.info("paused - DB thread");
//                doPause();
//            }
//            Gson gson = new Gson();
//            String json = gson.toJson(cursor.next());
//
//            JSONObject jsonObject = new JSONObject(json);
//            JSONObject properties = jsonObject.getJSONObject("properties");
//            properties.put("initial_data", "true");
//            json = jsonObject.toString();
//
//            sourceEventListener.onEvent(json , null);
//        }
//    }
//}
package io.siddhi.extension.io.live.source;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.live.source.Thread.AbstractThread;
import lombok.Builder;
//import org.json.JSONObject;
import net.minidev.json.JSONObject;

import java.sql.*;

@Builder
public class DBThread extends AbstractThread {
    private final SourceEventListener sourceEventListener;
    private String hostName;
    private int port;
    private String username;
    private String password;
    private String dbName;
    private String selectSQL;

    @Override
    public void run() {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            // Create a connection to the MySQL database
            String jdbcUrl = "jdbc:mysql://" + hostName + ":" + port + "/" + dbName;

            connection = DriverManager.getConnection(jdbcUrl, username, password);
            statement = connection.createStatement();
            // Execute the selectSQL query and process the results
            String select = selectSQL.replaceAll("@\\w+", "");
            LOGGER.info("query: "+select);
            resultSet = statement.executeQuery(select);
            while (resultSet.next() && isThreadRunning) {
                if (isPaused) {
                    LOGGER.info("paused - DB thread");
                    doPause();
                }
                // Convert the row to a JSON object and add the "initial_data" property
                JSONObject jsonObject = new JSONObject();
                int numColumns = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= numColumns; i++) {
                    String columnName = resultSet.getMetaData().getColumnName(i);
                    Object columnValue = resultSet.getObject(i);
                    LOGGER.info("col"+columnValue);
                    jsonObject.put(columnName, columnValue);
                }
                jsonObject.put("initial_data", "true");
                JSONObject jsonObject2 = new JSONObject(jsonObject);
                JSONObject properties = new JSONObject();
                properties.put("properties",jsonObject2);
                String json = properties.toString();
//
                // Send the event to the Siddhi source listener
                sourceEventListener.onEvent(json, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the database resources
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
