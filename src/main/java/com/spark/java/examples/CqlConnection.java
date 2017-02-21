package com.spark.java.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CqlConnection {

	public static void main(String[] args) throws Exception {

		Connection con = null;
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
			con = DriverManager.getConnection("jdbc:cassandra://localhost:9160/system");

			String query = "SELECT * FROM system ";

			Statement stmt = con.createStatement();
			ResultSet result = stmt.executeQuery(query);
			
			System.out.println(">>>>>>>>>>>> "+result);

			while (result.next()) {
				System.out.println(result.getString("user_name"));
				System.out.println(result.getString("gender"));
				System.out.println(result.getString("password"));
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				con = null;
			}
		}
	}
}
