package etg.hive.demo;

import java.sql.*;

public class HiveJDBC {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://server1:10000";
//    private static String user = "hadoop";
//    private static String password = "";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    public void init() throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
//        conn = DriverManager.getConnection(user, user, password);
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
    }

    public void createDataBase() throws SQLException {
        String sql = "create database if not exists hive_jdbc_test";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    public void showDatabase() throws SQLException {
        String sql = "show databases";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    public void createTable() throws SQLException {
        String sql = "create table if not EXISTS emp(" +
                "empno int," +
                "ename string," +
                "job string," +
                "hiredate string," +
                "sal double," +
                "comm double," +
                "deptno int) " +
                "row format delimited fields terminated by '\\t'";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    public void showTables() throws SQLException {
        String sql = "show tables";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    public void descTable() throws SQLException {
        String sql = "desc emp";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
    }

    public void loadData() throws SQLException {
        String filePath = "/home/hadoop/data/emp.txt";
        String sql = "load data local inpath '" + filePath + "' overwrite into table emp";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    public void selectData() throws SQLException {
        String sql = "select * from emp";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        System.out.println("员工编号\t员工姓名\t工作岗位");
        while (rs.next()) {
            System.out.println(rs.getString("empno") + "\t\t" + rs.getString("ename") + "\t\t" + rs.getString("job"));
        }
    }

    public void countData() throws SQLException {
        String sql = "select count(1) from emp";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
    }

    public void dropDatabase() throws SQLException {
        String sql = "drop database if exists hive_jdbc_test";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    public void dropTable() throws SQLException {
        String sql = "drop table if exists emp";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    public void destroy() throws SQLException {
        if (rs != null) rs.close();
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
    }


}
