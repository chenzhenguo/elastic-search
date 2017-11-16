package com.inspur.bigdata.manage.es.zsjexample;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * 跟业务无关的方法
 * 
 * @author zhaoshengjie
 *
 */
public class BaseUtil {
	static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	/**
	 * 返回uuid
	 * 
	 * @return
	 */
	public static String getUUid() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}

	/**
	 * 返回一个指定区间的随机事件
	 * 
	 * @return
	 */
	public static String getRandomTime(String startStr, String endStr) {

		try {
			Date start = format.parse(startStr);
			Date end = format.parse(endStr);// 构造结束日期
			long date = start.getTime() + (long) (Math.random() * (end.getTime() - start.getTime()));
			return format1.format(new Date(date));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // 构造开始日期
		return "2015-11-28 20:09:58";
	}


	// 几位随机整数
//	public static String getRandomInt(int length) {
//		String value = String.valueOf(RandomStringUtils.randomNumeric(length));
//		return value;
//	}

	// 两个整数之间随机取一个
	public static int getScopeInt(int min, int max) {
		Random random = new Random();
		int s = random.nextInt(max) % (max - min + 1) + min;
		return s;
	}
	
	public static void main(String[] args) {
		System.out.println(getScopeInt(1,3));
		System.out.println(getScopeInt(1,3));
		System.out.println(getScopeInt(1,3));
	}
}