package etg.hive.demo;

import java.sql.SQLException;

public class App {
    public static void main(String args[]) throws SQLException {
        HiveJDBC hiveJDBC = new HiveJDBC();
        try {
            hiveJDBC.init();
            System.out.println("显示数据库列表");
            hiveJDBC.showDatabase();
            System.out.println("新建数据库");
            hiveJDBC.createDataBase();
            System.out.println("显示数据库列表");
            hiveJDBC.showDatabase();
            System.out.println("新建数据库表");
            hiveJDBC.createTable();
            System.out.println("显示数据库表");
            hiveJDBC.showTables();
            hiveJDBC.descTable();
            hiveJDBC.selectData();

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            hiveJDBC.destroy();
        }
    }
}
