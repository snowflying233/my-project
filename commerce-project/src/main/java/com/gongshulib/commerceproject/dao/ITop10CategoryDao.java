package com.gongshulib.commerceproject.dao;

import com.gongshulib.commerceproject.domain.Top10Caregory;

public interface ITop10CategoryDao {

	/**
	 * 插入top10热门品类
	 * @param top10Caregory
	 */
	void insert(Top10Caregory top10Caregory);
}
