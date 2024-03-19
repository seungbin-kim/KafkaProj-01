package com.practice.kafka.consumer;

import java.sql.*;

public class JDBCTester {

    public static void main(String[] args) {

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement st = conn.createStatement()) {

            ResultSet rs = st.executeQuery("SELECT 'postgresql is connected' ");

            if (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
