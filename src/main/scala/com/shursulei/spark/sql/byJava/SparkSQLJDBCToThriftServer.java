package com.shursulei.spark.sql.byJava;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
 
import java.sql.*;
/**
 * 实战演示Java通过JDBC访问Thrift Server，进而访问SparkSQL，这是企业级开发中常用的方式
 * @author 18119
 *
 */
public class SparkSQLJDBCToThriftServer {
	public static void main(String[] args) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            String sql = "select name from people where age = ?";
            Connection connection = null;
            ResultSet resultSet = null;
            try {
                connection = DriverManager.getConnection("jdbc:hive2://Master:10001/default?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice", "root", "");
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setInt(1, 30);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1)); //此处的数据建议保存到Parquet等
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
