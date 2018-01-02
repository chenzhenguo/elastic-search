package com.inspur.bigdata.manage.es.zsjexample;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class EsTest {

	public static void main(String[] args) {

		String str = "hdfs://idapcluster/apps/hive/warehousr";

		System.out.println(str.replaceAll("[a-zA-Z]+://[a-zA-Z]+", ""));

		Map<String, String> map = new HashMap<String, String>();
		map.put("statusCode", "1");
		map.put("msgDesc", "NONE: is not a valid datamask-type. polic");

		String jsondata = JSON.toJSONString(map);

		JSONObject jr = JSONObject.parseObject(jsondata);

		if (jr.get("msgDesc") != null) {
			System.out.println(jr.get("msgDesc"));
		} else {
			System.out.println("true");
		}

		String regu1 = "(?<!c)a(\\d+)b";
		Pattern p = Pattern.compile(regu1);
		Matcher m = p.matcher("da12bca3434bdca4356bdca234bm");

		if (m.find()) {
			System.out.println(m.group(1));

			System.out.println(m.group(0));

		}

		Pattern p1 = Pattern.compile("(?<=[w]{3}[.]{1})([A-Za-z0-9]+)(?=[.]com)");
		Matcher m1=p1.matcher("www.wjnb.com");
		if(m1.find()){
			System.out.println(m1.group(1));
			System.out.println(m1.group(0));
			System.out.println(m1.groupCount());
			
		}

	}

}
