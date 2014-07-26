package com.sidooo;

import java.io.*;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class Main {

    public static void usage() {
        System.out.println("qqun_export <host> <database>");
    }

    public static String[] listTableName(Connection conn) {

        Set<String> names = new HashSet<String>();

        try {
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables(null, null, null, new String[]{"TABLE"});
            while(rs.next()) {
                 names.add(rs.getString(3));
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return names.toArray(new String[1]);
    }

    private static boolean exportTable(ResultSet rs, PrintWriter output) {
        try {
            ResultSetMetaData meta = rs.getMetaData();

            int columnCount = meta.getColumnCount();
            String title = "";
            for(int i=1; i<=columnCount; i++) {
                if (i!=columnCount) {
                    title += meta.getColumnName(i)+",";
                } else {
                    title += meta.getColumnName(i);
                }
            }

            output.println(title);
            long  count=0;
            while(rs.next()) {

                count++;
                if (count % 1000 == 0) {
                    System.out.println("Export "+count+" records.");
                }
                String line = "";
                for(int i=1; i<=columnCount; i++) {
                     String typeName = meta.getColumnTypeName(i);
                      if ("int".equals(typeName)) {
                            line += rs.getInt(i);
                      } else if ("varchar".equals(typeName)) {
                            line += rs.getString(i);
                      } else {
                            throw new Exception();
                      }

                      if (i != columnCount) {
                          line += ",";
                      }
                }

                output.println(line);
            }

            return true;
        } catch(Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
	// write your code here

        String ip = args[0];
        String database = args[1];

        if (args.length != 2) {
            usage();
            return;
        }

        System.out.println("Host: " + ip);
        System.out.println("Database: "+database);

        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        String url = "jdbc:sqlserver://127.0.0.1:1433; DatabaseName="+database;
        String user="sa";
        String password = "111";

        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, user, password);

            if (!conn.isClosed()) {
                System.out.println("Succeeded connection to the Database!");

                String[] tableNames = listTableName(conn);
                for (String table : tableNames) {


                    String sql = "select * from " + table;
                    conn.setAutoCommit(false);
                    PreparedStatement stmt =
                            conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
                    stmt.setFetchSize(50);
                    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
                    //stmt.setMaxRows(100);
                    stmt.execute();
                    ResultSet rs = stmt.getResultSet();

                    String outputFileName = database+"-"+table+".csv";
                    FileOutputStream fos  = new FileOutputStream(outputFileName);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                    PrintWriter output = new PrintWriter(new BufferedWriter(osw));
                    boolean ret = exportTable(rs, output);
                    output.close();
                    rs.close();
                    stmt.close();

                    if (!ret) {
                        throw new Exception("exportTable Failed!");
                    }
                }

                conn.close();
            }

        } catch(ClassNotFoundException e) {
            e.printStackTrace();
        } catch(SQLException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
