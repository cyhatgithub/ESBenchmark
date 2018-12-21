import cn.thread.bulk.JavaBulk;
import cn.thread.bulk.Monitor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by chenyinghao on 2018/9/26.
 */
public class Main {
    public static Monitor monitor = new Monitor();

    public static void main(String[] args) throws IOException {
//        InputStream is = Main.class.getClassLoader().getResourceAsStream("bulk.conf");
        InputStream is = new FileInputStream(args[0]);
        final Properties properties = new Properties();
        properties.load(is);
        final String esHost = properties.getProperty("elasticsearch.host").trim();
        final String clusterName = properties.getProperty("elasticsearch.cluster.name").trim();
        final int esPort = Integer.parseInt(properties.getProperty("elasticsearch.port").trim());

        final int totalThread = Integer.parseInt(properties.getProperty("bulk.thread.size"));
        final int onceCommitSize = Integer.parseInt(properties.getProperty("bulk.commit.size"));
        final String reportPath = properties.getProperty("report.path");
        monitor.setMonitorProperties(reportPath, onceCommitSize, totalThread);
        // max support 20 thread to bulk
        List<String> pathList = Arrays.asList(
            properties.getProperty("bulk.file.path0").trim(),
            properties.getProperty("bulk.file.path1").trim(),
            properties.getProperty("bulk.file.path2").trim(),
            properties.getProperty("bulk.file.path3").trim(),
            properties.getProperty("bulk.file.path4").trim(),
            properties.getProperty("bulk.file.path5").trim(),
            properties.getProperty("bulk.file.path6").trim(),
            properties.getProperty("bulk.file.path7").trim(),
            properties.getProperty("bulk.file.path8").trim(),
            properties.getProperty("bulk.file.path9").trim(),
            properties.getProperty("bulk.file.path10").trim(),
            properties.getProperty("bulk.file.path11").trim(),
            properties.getProperty("bulk.file.path12").trim(),
            properties.getProperty("bulk.file.path13").trim(),
            properties.getProperty("bulk.file.path14").trim(),
            properties.getProperty("bulk.file.path15").trim(),
            properties.getProperty("bulk.file.path16").trim(),
            properties.getProperty("bulk.file.path17").trim(),
            properties.getProperty("bulk.file.path18").trim(),
            properties.getProperty("bulk.file.path19").trim()
        );

        List<String> indexList = Arrays.asList(
            properties.getProperty("bulk.path.index0").trim(),
            properties.getProperty("bulk.path.index1").trim(),
            properties.getProperty("bulk.path.index2").trim(),
            properties.getProperty("bulk.path.index3").trim(),
            properties.getProperty("bulk.path.index4").trim(),
            properties.getProperty("bulk.path.index5").trim(),
            properties.getProperty("bulk.path.index6").trim(),
            properties.getProperty("bulk.path.index7").trim(),
            properties.getProperty("bulk.path.index8").trim(),
            properties.getProperty("bulk.path.index9").trim(),
            properties.getProperty("bulk.path.index10").trim(),
            properties.getProperty("bulk.path.index11").trim(),
            properties.getProperty("bulk.path.index12").trim(),
            properties.getProperty("bulk.path.index13").trim(),
            properties.getProperty("bulk.path.index14").trim(),
            properties.getProperty("bulk.path.index15").trim(),
            properties.getProperty("bulk.path.index16").trim(),
            properties.getProperty("bulk.path.index17").trim(),
            properties.getProperty("bulk.path.index18").trim(),
            properties.getProperty("bulk.path.index19").trim()
        );

        List<JavaBulk> bulks = new ArrayList<JavaBulk>();
        for(int i = 0; i < totalThread; i++) {
            bulks.add(new JavaBulk(esHost, esPort, clusterName, indexList.get(i), pathList.get(i), onceCommitSize, monitor));
        }
        for(JavaBulk bulk : bulks) {
            new Thread(bulk).start();
        }

//        JavaBulk javaBulk0 = new JavaBulk(esHost, esPort, clusterName, index0, path0);
//        JavaBulk javaBulk1 = new JavaBulk(esHost, esPort, clusterName, index1, path1);
//        JavaBulk javaBulk2 = new JavaBulk(esHost, esPort, clusterName, index2, path2);
//        JavaBulk javaBulk3 = new JavaBulk(esHost, esPort, clusterName, index3, path3);
//        JavaBulk javaBulk4 = new JavaBulk(esHost, esPort, clusterName, index4, path4);

//        javaBulk1.bulkInsert("single");
//        new Thread(javaBulk1).start();
//        new Thread(javaBulk2).start();
//        new Thread(javaBulk3).start();
//        new Thread(javaBulk4).start();
//        new Thread(javaBulk0).start();
//
//        new Thread(javaBulk1).start();
//        new Thread(javaBulk2).start();
//        new Thread(javaBulk3).start();
//        new Thread(javaBulk4).start();
//        new Thread(javaBulk0).start();

    }
}
