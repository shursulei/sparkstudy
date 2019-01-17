package com.shursulei.spark.sql.weblog.byJava;
/**
 * 计算PV、UV、热门板块、跳出率、新用户注册比率
 */
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
/**
 * Table in hive database creation:
 * sqlContext.sql("create table userlogs(date string, timestamp bigint, userID bigint, pageID bigint, channel string, action string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'")
 *
 */
public class SparkSQLUserlogsOps {
	 /**
     * @param args
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkSQLUserlogsOps").setMaster("spark://Master:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        HiveContext hiveContext = new HiveContext(sc);
        String yesterday = getYesterday();
        pvStat(hiveContext, yesterday); //PV
        uvStat(hiveContext, yesterday); //UV
        hotChannel(hiveContext, yesterday); //热门板块
        jumpOutStat(hiveContext, yesterday); //跳出率
        newUserRegisterPercentStat(hiveContext, yesterday); //新用户注册的比例
    }

    private static void newUserRegisterPercentStat(HiveContext hiveContext, String yesterday) {
        hiveContext.sql("use hive");
        String newUserSQL = "select count(*) "
                + "from userlogs "
                + "where action = 'View' and date='"+ yesterday+"' and userID is NULL "
//              + "limit 10"
                ;

        String RegisterUserSQL = "SELECT count(*) "
                + "from userlogs"
                + "where action = 'Register' and date='"+ yesterday+"' "
//              + "limit 10"
                ;
        Object newUser = hiveContext.sql(newUserSQL).collect()[0].get(0);
//        List<Row> newUserlist=hiveContext.sql(newUserSQL).collectAsList();
        Object RegisterUser = hiveContext.sql(RegisterUserSQL).collect()[0].get(0);
        double total = Double.valueOf(newUser.toString());
        double register = Double.valueOf(RegisterUser.toString());
        System.out.println("模拟新用户注册比例：" + register / total);
    }

    private static void jumpOutStat(HiveContext hiveContext, String yesterday) {
        hiveContext.sql("use hive");
        String totalPvSQL = "select count(*) "
                + "from "
                + "userlogs "
                + "where action = 'View' and date='"+ yesterday+"' "
//              + "limit 10"
                ;
        String pv2OneSQL = "SELECT count(*) "
                + "from "
                + "(SELECT count(*) totalNumber from userlogs "
                + "where action = 'View' and date='"+ yesterday+"' "
                + "group by userID "
                + "having totalNumber = 1) subquery "
//              + "limit 10"
                ;
        Object totalPv = hiveContext.sql(totalPvSQL).collect()[0].get(0);
        Object pv2One = hiveContext.sql(pv2OneSQL).collect()[0].get(0);
        double total = Double.valueOf(totalPv.toString());
        double pv21 = Double.valueOf(pv2One.toString());
        System.out.println("跳出率为" + pv21 / total);
    }
    private static void uvStat(HiveContext hiveContext, String yesterday) {
        // TODO Auto-generated method stub
        hiveContext.sql("use hive");

        String sqlText = "select date, pageID, uv "
                + "from "
                + "(select date, pageID, count(distinct(userID)) uv from userlogs "
                + "where action = 'View' and date='"+ yesterday+"' "
                + "group by date, pageID) subquery "
                + "order by uv desc "
//              + "limit 10"
                ;

        hiveContext.sql(sqlText).show();
    }
    private static void hotChannel(HiveContext hiveContext, String yesterday) {
        // TODO Auto-generated method stub
        hiveContext.sql("use hive");

        String sqlText = "select date, pageID, pv "
                + "from "
                + "(select date, pageID, count(1) pv from userlogs "
                + "where action = 'View' and date='"+ yesterday+"' "
                + "group by date, pageID) subquery "
                + "order by pv desc "
//              + "limit 10"
                ;
        hiveContext.sql(sqlText).show();
    }

    private static void pvStat(HiveContext hiveContext, String yesterday) {
        hiveContext.sql("use hive");
        String sqlText = "select date, channel, channelpv "
                + "from "
                + "(select date, channel, count(*) channelpv from userlogs "
                + "where action = 'View' and date='"+ yesterday+"' "
                + "group by date, channel) subquery "
                + "order by channelpv desc "
//              + "limit 10"
                ;
        hiveContext.sql(sqlText).show();
        //把执行结果放到数据库或Hive中
        //select date, pageID, pv from (select date, pageID, count(1) pv from userlogs where action = 'View' and 
        //date='2017-03-10' group by date, pageID) subquery order by pv desc limit 10   
    }
    private static String getYesterday() {
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -2);
        Date yesterday = cal.getTime();
        return date.format(yesterday);
    }
}
