package com.gongshulib.commerceproject.dao;

import com.gongshulib.commerceproject.domain.Task;

/**
 * 任务管理Dao接口
 * DAO　Data Access Object数据访问对象
 * 也可以称作数据持久层
 * 主要是针对数据库中数据的储存或读写
 * @author Administrator
 *
 */
public interface ITaskDao {
	/**
	 * 根据主键查询任务
	 * @param taskid
	 * @return Task
	 */
	Task findById(long taskid);
}
