package cn.thread.bulk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by chenyinghao on 2018/9/27.
 */
public class Monitor {
    public static int totalThread;  //  多少进程正在插入
    public static long totalCommit; //  总插图条数
    public static long duration;    //  插入任务持续时间(总)

    public static long nowBulkThroughPut_all;   //  此刻(总)吞吐量
    public static long maxBulkThroughPut_all;   //  最大(总)吞吐量
    public static long minBulkThroughPut_all;   //  最小(总)吞吐量

    public static long nowBulkThroughPut_single;    //  当前进程吞吐量
    public static long maxBulkThroughPut_single;    //  所有进程中最大吞吐量
    public static long minBulkThroughPut_single;    //  所有进程中最小吞吐量

    public static double totalCommitTimes;  //  总批量插入次数
    public static double commitSuccessTimes;    //  总成功次数
    public static double commitFailTimes;   //  总失败次数

    public static long onceCommitSize;  //  单次插入数量

    public static String reportPath;    //  监控报告路径

    public static long startMonitorMs = System.currentTimeMillis();

    private static Logger logger = LoggerFactory.getLogger(Monitor.class);

    public static void setMonitorProperties(String reportPath1, long onceCommitSize1, int totalThread1) {
        reportPath = reportPath1;
        onceCommitSize = onceCommitSize1;
        totalThread = totalThread1;
    }

    //  成功批量插入更新监控参数
    public static void updateThroughputSucRecord(long singleDuration) {

        long thisBulkThroughput = (onceCommitSize * 1000)/singleDuration;
        //  更新单个进程最大最小吞吐量
        if(minBulkThroughPut_single == 0) {
            minBulkThroughPut_single = thisBulkThroughput;
        }else {
            if(thisBulkThroughput < minBulkThroughPut_single) {
                minBulkThroughPut_single = thisBulkThroughput;
            }
        }

        if(maxBulkThroughPut_single < thisBulkThroughput) {
            maxBulkThroughPut_single = thisBulkThroughput;
        }
        nowBulkThroughPut_single = thisBulkThroughput;

        //  更新总的吞吐量相关监控数据
        duration = System.currentTimeMillis() - startMonitorMs;
        totalCommit = totalCommit + onceCommitSize;
        long thisBulkThroughPut_all = (totalCommit * 1000) / duration;
        if(minBulkThroughPut_all == 0) {
            minBulkThroughPut_all = thisBulkThroughPut_all;
        }else {
            if(thisBulkThroughPut_all < minBulkThroughPut_all) {
                minBulkThroughPut_all = thisBulkThroughPut_all;
            }
        }
        if(maxBulkThroughPut_all < thisBulkThroughPut_all) {
            maxBulkThroughPut_all = thisBulkThroughPut_all;
        }
        nowBulkThroughPut_all = thisBulkThroughPut_all;

        totalCommitTimes ++;
        commitSuccessTimes ++;
        report();
    }

    //  批量插入失败更新监控参数
    public static void updateThroughputFailRecord() {
        nowBulkThroughPut_single = -1;
        nowBulkThroughPut_all = -1;
        totalCommitTimes ++;
        commitFailTimes ++;
        report();
    }



    //  输出本次任务结果
    public static void report() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("totalThread:" + totalThread + "\t");
        buffer.append("totalCommit:" + totalCommit + "docs\t");
        buffer.append("duration:" + duration + "ms\n");
        buffer.append("commitFailPercent:" + commitFailTimes * 100/totalCommitTimes + "%\t");
        buffer.append("totalCommitTimes:" + totalCommitTimes + "\t\n");
        buffer.append("nowBulkThroughPut_single:" + nowBulkThroughPut_single + "docs/s\t");
        buffer.append("minBulkThroughPut_single:" + minBulkThroughPut_single + "docs/s\t");
        buffer.append("maxBulkThroughPut_single:" + maxBulkThroughPut_single + "docs/s\t\n");
        buffer.append("nowBulkThroughPut_all:" + nowBulkThroughPut_all + "docs/s\t");
        buffer.append("minBulkThroughPut_all:" + minBulkThroughPut_all + "docs/s\t");
        buffer.append("maxBulkThroughPut_single:" + maxBulkThroughPut_all + "docs/s\n");
        buffer.append("------------------------------------------------------------------------------------------------------------------------\n");
        try {
            File file = new File(reportPath);
            if(!file.exists()) {
                File dir = new File(file.getParent());
                dir.mkdirs();
                file.createNewFile();
            }

            // 打开一个随机访问文件流，按读写方式
            RandomAccessFile randomFile = new RandomAccessFile(reportPath, "rw");
            // 文件长度，字节数
            long fileLength = randomFile.length();
            //将写文件指针移到文件尾
            randomFile.seek(fileLength);
            randomFile.writeBytes(buffer.toString());
            randomFile.close();

        }catch (IOException ioe) {
            logger.info("write report fail!");
        }

    }
}
