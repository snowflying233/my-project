package com.gongshulib.commerceproject.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC 增删改查
 * @author Administrator
 *
 */
public class JdbcCRUD {
	public static void main(String[] args) {
		
		//insert();
		prepareStatement();
	}
	
	@SuppressWarnings("unused")
	private static void insert(){
		
		Connection conn = null;
		Statement stmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/commerce_project",
					"root",
					"000000");
			stmt = conn.createStatement();
			
			String sql = "insert into test_user(id,name,age) values(4,'zhaoliu',18)";
			int result = stmt.executeUpdate(sql);
			
			System.out.println("影响了几行：" + result);
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (stmt != null){
					stmt.close();
				}
				if (conn !=  null){
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	/**
	 * 测试prepareStatement
	 * 
	 */
	private static void prepareStatement(){
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/commerce_project",
					"root",
					"000000");
			
			String sql = "insert into test_user(id,name,age) values(?,?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, 6);
			pstmt.setString(2, "张");
			pstmt.setInt(3, 18);
			
			int result = pstmt.executeUpdate();
			System.out.println("影响了几行：" + result);
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (pstmt != null){
					pstmt.close();
				}
				if (conn !=  null){
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		
	}
	
}
