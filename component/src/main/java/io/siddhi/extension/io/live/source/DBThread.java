package io.siddhi.extension.io.live.source;

import com.c8db.C8Cursor;
import com.c8db.C8DB;
import com.c8db.entity.BaseDocument;
import com.google.gson.Gson;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.live.source.Thread.AbstractThread;
import io.siddhi.extension.io.live.utils.Monitor;
//import net.minidev.json.JSONObject;
import lombok.Builder;
import org.apache.tapestry5.json.JSONObject;

@Builder
public class DBThread extends AbstractThread {
    private final SourceEventListener sourceEventListener;
    private String hostName;
    private int port = 443;
    private String apiKey;
    private String user = "root";
    private String selectSQL;

    @Override
    public void run() {
        final C8DB c8db = new C8DB.Builder().useSsl(true).host(hostName , port).apiKey(apiKey).user(user).build();
        final C8Cursor<BaseDocument> cursor = c8db.db(null , "_system")
                .query(selectSQL, null, null, BaseDocument.class);

        while (cursor.hasNext() && isThreadRunning) {
            if(isPaused) {
                System.out.println("paused - DB thread");
                doPause();
            }
            Gson gson = new Gson();
            String json = gson.toJson(cursor.next());

            JSONObject jsonObject = new JSONObject(json);
            JSONObject properties = jsonObject.getJSONObject("properties");
            properties.put("initial_data", "true");
            json = jsonObject.toString();

            sourceEventListener.onEvent(json , null);
        }
    }
}