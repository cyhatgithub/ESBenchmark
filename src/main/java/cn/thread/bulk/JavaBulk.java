package cn.thread.bulk;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chenyinghao on 2018/9/25.
 */
public class JavaBulk extends ElasticsearchConnector implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(JavaBulk.class);

    public static final String PATTERN_REGEX = "^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{1,2}:\\d{1,2}.\\d{3}Z";
    public static final Pattern PATTERN = Pattern.compile(PATTERN_REGEX);

    public String indexName;
    public String dataPath;

    public int onceCommitSize;

    public Monitor monitor;

    public JavaBulk(String esHosts, int esPort, String clusterName, String indexName, String dataPath, int onceCommitSize, Monitor monitor) {
        super(esHosts, esPort, clusterName);
        this.indexName = indexName;
        this.dataPath = dataPath;
        this.onceCommitSize = onceCommitSize;
        this.monitor = monitor;
    }

    public void run() {
        System.out.println(Thread.currentThread().getName() + " start");
        try {
            while (true) {
                bulkInsert(Thread.currentThread().getName());
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void bulkInsert(String name) throws IOException {
        int commitTime = 0;
        FileInputStream fileInputStream = new FileInputStream(dataPath);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line ;
        try {
            long offset = 0l;
            int patternCount = 0;
            String lastDate = "";
            int docCount = 0;
            StringBuffer logBuffer = new StringBuffer();
            BulkRequestBuilder bulkRequestBuilder = this.client.prepareBulk();

            while ((line = bufferedReader.readLine())!=null) {
                offset++;

                Matcher matcher = PATTERN.matcher(line);
                if(matcher.find()) {
                    patternCount ++;
                    if(patternCount == 1) {
                        logBuffer.append(line);
                    }else {
                        final String date = lastDate;
                        final String message = logBuffer.toString();
                        final long offset1 = offset;
                        Map<String, Object> reqBody = new HashMap<String, Object>() {{
                            put("beat", new HashMap<String, String>(){{ put("version", "5.6.5"); }});
                            put("host", "centos24");
                            put("inputType", "log");
                            put("ip", "192.168.1.204");
                            put("loglevel", "INFO");
                            put("orgId", 1);
                            put("sysId", 1);
                            put("source", "/data/alertd.log");
                            put("type", "alertd");
                            put("@timestamp", date);
                            put("message", message);
                            put("offset", offset1);
                        }};
                        bulkRequestBuilder.add(this.client.prepareIndex(indexName, "alertd").setSource(reqBody));
                        logBuffer = new StringBuffer();
                        patternCount = 0;
                    }
                    lastDate = matcher.group(0);
                    docCount ++;
                }else {
                    logBuffer.append(line);
                }

                if(docCount % onceCommitSize == 0 && offset != 0) {

                    long start1 = System.currentTimeMillis();
                    BulkResponse responses = bulkRequestBuilder.execute().actionGet();
                    long end = System.currentTimeMillis();
                    long duration = end - start1;
                    if(!responses.hasFailures()) {
                        monitor.updateThroughputSucRecord(duration);

                        commitTime ++;
                        if(commitTime % 3 == 0 && commitTime != 0)
                            reconnect();
                        bulkRequestBuilder = this.client.prepareBulk();
                    }else {
                        monitor.updateThroughputFailRecord();
                    }
                }
            }
        }catch (Exception e) {
            logger.error("insert into ES by bulk fail", e);
        }finally {
            bufferedReader.close();
            inputStreamReader.close();
            fileInputStream.close();
        }

    }

}