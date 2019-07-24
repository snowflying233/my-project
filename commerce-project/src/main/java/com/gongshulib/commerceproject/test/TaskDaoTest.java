package com.gongshulib.commerceproject.test;

import com.gongshulib.commerceproject.dao.ITaskDao;
import com.gongshulib.commerceproject.dao.factory.DAOFactory;
import com.gongshulib.commerceproject.domain.Task;

/**
 * 任务管理DAO测试类
 * @author Administrator
 *
 */
public class TaskDaoTest {
	public static void main(String[] args) {
		ITaskDao taskDao = DAOFactory.getTaskDao();
		
		Task task = taskDao.findById(1);
		
		System.out.println(task.getTaskName());
	}
}
