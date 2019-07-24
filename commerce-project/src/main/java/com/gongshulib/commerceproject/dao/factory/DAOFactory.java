package com.gongshulib.commerceproject.dao.factory;

import com.gongshulib.commerceproject.dao.IPageSplitConvertRateDao;
import com.gongshulib.commerceproject.dao.ISessionAggrStatDao;
import com.gongshulib.commerceproject.dao.ISessionDetailDao;
import com.gongshulib.commerceproject.dao.ITaskDao;
import com.gongshulib.commerceproject.dao.ITop10CategoryDao;
import com.gongshulib.commerceproject.dao.ITop10SessionDao;
import com.gongshulib.commerceproject.dao.impl.PageSplitConvertRateImpl;
import com.gongshulib.commerceproject.dao.impl.SessionAggrStatDaoImpl;
import com.gongshulib.commerceproject.dao.impl.SessionDetailDaoImpl;
import com.gongshulib.commerceproject.dao.impl.SessionRandomExtractDaoImpl;
import com.gongshulib.commerceproject.dao.impl.TaskDaoImpl;
import com.gongshulib.commerceproject.dao.impl.Top10CategoryDaoImpl;
import com.gongshulib.commerceproject.dao.impl.Top10SessionDaoImpl;
import com.gongshulib.commerceproject.dao.ISessionRandomExtractDao;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {
	
	/**
	 * 获取任务管理DAO
	 * @return
	 */
	public static ITaskDao getTaskDao(){
		return new TaskDaoImpl();
	}
	
	/**
	 * 获取session聚合统计Dao
	 * @return
	 */
	public static ISessionAggrStatDao getSessionAggrStatDao(){
		return new SessionAggrStatDaoImpl();
	}
	
	/**
	 * 获取session随机抽取SessionRandomExtractDao
	 * @return
	 */
	public static ISessionRandomExtractDao getSessionRandomExtractDao(){
		return new SessionRandomExtractDaoImpl();
	}
	
	/**
	 * 获取session插入明细Dao
	 * @return
	 */
	public static ISessionDetailDao getSessionDetailDao(){
		return new SessionDetailDaoImpl();
	}
	
	/**
	 * 获取top10热门品类Dao
	 * @return
	 */
	public static ITop10CategoryDao getTop10CategoryDao(){
		return new Top10CategoryDaoImpl();
	}
	
	public static ITop10SessionDao getTop10SessionDao(){
		return new Top10SessionDaoImpl();
	}
	
	public static IPageSplitConvertRateDao getPageSplitConvertRateDao(){
		return new PageSplitConvertRateImpl();
	}
}
