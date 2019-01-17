package com.shursulei.spark.sql.weblog.byJava;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * 生成数据 SparkSQLDataManually.java
 * 论坛数据自动生成代码，数据格式如下：
 * data:日期，格式为yyyy-MM-dd
 * timestamp:时间戳
 * userID:用户ID
 * pageID:页面ID
 * channelID:板块ID
 * action:点击和注册
 */
public class SparkSQLDataManually {
	static String yesterday = yesterday();
    static String[] channelNames = new String[] {
            "Spark", "Scala", "Kafka", "Flink", "Hadoop", "Storm",
            "Hive", "Impala", "HBase", "ML"
    };
    static String[] actionNames = new String[] {
        "register",//注册
        "view"//点击
    };
    public static void main(String[] args) {
        /**
         * 通过传递进来的参数生成制定大小规模的数据
         */
        long numberItems = 5000;
        String path = ".";
        if (args.length > 0) {
            numberItems = Integer.valueOf(args[0]);
            path = args[1];
            System.out.println(path);
        }

        System.out.println("User log number is : " + numberItems);
        //具体的论坛频道
        /**
         * 昨天的时间生成
         */
        userlogs(numberItems, path);
    }

    private static void userlogs(long numberItems, String path) {
        Random random = new Random();
        StringBuffer userLogBuffer = new StringBuffer("");
        int[] unregisteredUsers = new int[]{1,2,3,4,5,6,7,8};
        for(int i = 0; i < numberItems; i++) {
            long timestamp = new Date().getTime();
            Long userID = 0L;
            long pageID = 0;
            //随机生成的用户ID
            if(unregisteredUsers[random.nextInt(8)] == 1) {
                userID = null;
            }
            else {
                userID = (long) random.nextInt((int) numberItems);
            }
            //随机生成的页面ID
            pageID = random.nextInt((int) numberItems);
            //随机生成Channel
            String channel = channelNames[random.nextInt(10)];
            //随机生成acton行为
            String action = actionNames[random.nextInt(2)];
            userLogBuffer.append(yesterday)
                .append("\t")
                .append(timestamp)
                .append("\t")
                .append(userID)
                .append("\t")
                .append(pageID)
                .append("\t")
                .append(channel)
                .append("\t")
                .append(action)
                .append("\n");

        }
//      System.out.print(userLogBuffer);
        PrintWriter pw = null;

        try {
            pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path + "\\userlog.log")));
            System.out.println(path + "userlog.log");
            pw.write(userLogBuffer.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pw.close();
        }
    }
    private static String yesterday() {
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);
        Date yesterday = cal.getTime();
        return date.format(yesterday);
    }

}
