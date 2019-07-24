package com.gongshulib.commerceproject.test;

import java.util.ArrayList;
import java.util.List;

import com.gongshulib.commerceproject.jdbc.JDBCHelper;

public class JDBCHelperTest {
	public static void main(String[] args) throws Exception {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		/*
		//测试普通的增删改
		int rtn = jdbcHelper.executeUpdate(
				"insert into test_user(id,name,age) values(?,?,?)",
				new Object[]{9,"dsiqi",21});
		System.out.println(rtn);
		*/
		
		/*
		//测试查询
		jdbcHelper.executeQuery(
				"select * from test_user where id=?",
				new Object[]{7},
				new JDBCHelper.QueryCallback() {
					
					@Override
					public void process(ResultSet rs) throws Exception {
						if (rs.next()){
							String name = rs.getString(2);
							int age = rs.getInt(3);
							
							System.out.println("name:" + name +" age:" + age);
						}
					}
				});
				
		*/
		
		//测试批量执行SQL语句
		String sql = "insert into test_user(id,name,age) values(?,?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		paramsList.add(new Object[]{10,"wangwu3",20});
		paramsList.add(new Object[]{11,"zhaoliu2",23});
		
		int[] rtn = jdbcHelper.executeBatch(sql,paramsList);
		for (int i : rtn) {
			System.out.println(i);
		}
	}
}
