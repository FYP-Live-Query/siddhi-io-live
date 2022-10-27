package io.siddhi.extension.io.live.source;

import com.c8db.C8Cursor;
import com.c8db.C8DB;
import com.c8db.entity.BaseDocument;
import com.google.gson.Gson;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.live.source.Thread.AbstractThread;
import io.siddhi.extension.io.live.utils.Monitor;

public class DBThread extends AbstractThread {
    private final SourceEventListener sourceEventListener;
    private String hostName;
    private int port = 443;
    private String apiKey;
    private String user = "root";
    private String selectSQL;

    public DBThread(Monitor interThreadSignalMonitor, SourceEventListener sourceEventListener, String hostName, String apiKey, String user, String selectSQL) {
        super(interThreadSignalMonitor);
        this.sourceEventListener = sourceEventListener;
        this.hostName = hostName;
        this.apiKey = apiKey;
        this.user = user;
        this.selectSQL = selectSQL;
    }

    public DBThread(Monitor interThreadSignalMonitor, SourceEventListener sourceEventListener, String hostName, String apiKey, String selectSQL) {
        super(interThreadSignalMonitor);
        this.sourceEventListener = sourceEventListener;
        this.hostName = hostName;
        this.apiKey = apiKey;
        this.selectSQL = selectSQL;
    }

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
            System.out.println(json);
            sourceEventListener.onEvent(json , null);
        }
    }
}