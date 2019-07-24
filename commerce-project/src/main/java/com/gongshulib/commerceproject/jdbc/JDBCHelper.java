package com.gongshulib.commerceproject.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import com.gongshulib.commerceproject.conf.ConfigurationManager;
import com.gongshulib.commerceproject.constant.Constants;

/**
 * 创建指定数量的数据库连接池
 * 通过单例模式创建
 * @author Administrator
 *
 */
public class JDBCHelper {
	
	static {
		try {
			String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
			Class.forName(driver);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static JDBCHelper instance = null;
	
	/**
	 * 获取JDBCHelper对象的单例
	 * 保证JDBCHelper只有一个实例
	 * @return
	 */
	public static JDBCHelper getInstance() {
		//两步检查机制
		if (instance == null) {
			synchronized(JDBCHelper.class) {
				if (instance == null) {
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}
	
	//存放数据库连接
	//LinkedList数组特性，随机访问的效率比较低（和ArrayList相比）
	//LinkedList添加或者删除的效率较高
	//我们从数组中获取数据库连接，并不需要随机访问，只需要获取一个连接即可，获取到连接后从数组中移除，使用完毕后再把连接添加进来（push）
	//选择LinkedList的效率更高
	private LinkedList<Connection> datasource = new LinkedList<Connection>();
	
	/**
	 * JDBCHelper创建唯一一份数据库连接池
	 * 
	 */
	private JDBCHelper() {
		//数据库连接池大小
		int datasourceSize = ConfigurationManager.getInteger(
				Constants.JDBC_DATASOURCE_SIZE);
		//循环创建数据库连接
		for (int i=0; i<datasourceSize; i++){
			
			Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
			String url = null;
			String user = null;
			String password = null;
			
			
			if(local){
				url = ConfigurationManager.getProperty(Constants.JDBC_URL);
				user = ConfigurationManager.getProperty(Constants.JDBC_USER);
				password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
			}else {	
				url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
				user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
				password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
			}
			
			try {
				Connection conn = DriverManager.getConnection(url, user, password);
				//创建好的连接放入准备好的 LinkedList<Connection> datasource 中
				datasource.push(conn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		
		}
	}
	
	/**
	 * 
	 * 排队获取数据库连接
	 * 如果数据库连接池中连接已用完就等待10毫秒
	 * @return
	 */
	public synchronized Connection getConnection(){
		while(datasource.size() == 0){
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//poll() 获取并移除队列头部元素，如果队列为空，返回null；
		return datasource.poll();
	}
	
	/**
	 * 开发增删改的方法
	 * 1.执行增删改SQL语句的方法
	 * 2.执行查询SQL语句的方法
	 * 3.批量执行SQL语句的方法
	 */
	
	/**
	 * 执行增删改SQL语句
	 * @param sql
	 * @param params
	 * @return 影响的行数
	 */
	public int executeUpdate(String sql, Object[] params) {
		int rtn = 0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			
			for(int i=0; i<params.length; i++){
				pstmt.setObject(i+1, params[i]);
			}
			
			rtn = pstmt.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null){
				//用完连接还回数据库连接池
				datasource.push(conn);
			}
		}
		
		return rtn;
	}
	
	/**
	 * 执行查询SQL语句
	 * @param sql
	 * @param 
	 * @param callback
	 */
	public void executeQuery(String sql, Object[] params, 
			QueryCallback callback){
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			
			for(int i=0; i<params.length; i++){
				pstmt.setObject(i+1, params[i]);
			}
			
			rs = pstmt.executeQuery();
			
			//自定义处理查询结果
			callback.process(rs);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null){
				datasource.push(conn);
			}
		}
	}

	//批量执行SQL语句
	public int[] executeBatch(String sql, List<Object[]> paramsList){
		int[] rtn = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			conn = getConnection();
			
			//第一步：关闭自动提交
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			
			//第二步：使用PreparedStatement.addBatch() 方法，加入批量的SQL参数
			for (Object[] params : paramsList) {
				for(int i=0; i<params.length; i++){
					pstmt.setObject(i+1, params[i]);
				}
				//一组参数设置完，添加一个Batch()
				pstmt.addBatch();
			}
			
			//第三步：PreparedStatement.executeBatch() 方法，执行批量的SQL语句
			rtn = pstmt.executeBatch();
			
			//最后一步：使用Connection对象，批量提交SQL语句
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null){
				datasource.push(conn);
			}
		}
		
		return rtn;
	}
	
	/**
	 * 静态内部类：查询回调接口
	 * @author Administrator
	 *
	 */
	public static interface QueryCallback{
		/**
		 * 处理查询结果
		 * @param rs
		 * @throws Exception
		 */
		void process(ResultSet rs) throws Exception;
	}
	
}
