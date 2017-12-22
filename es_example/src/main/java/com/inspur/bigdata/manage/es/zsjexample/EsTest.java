package com.inspur.bigdata.manage.es.zsjexample;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class EsTest {

	public static void main(String[] args) {

		String str = "hdfs://idapcluster/apps/hive/warehousr";

		System.out.println(str.replaceAll("[a-zA-Z]+://[a-zA-Z]+", ""));
		
		
		
		Map<String,String> map=new HashMap<String,String>();
		map.put("statusCode", "1");
		map.put("msgDesc", "NONE: is not a valid datamask-type. polic");
		
		
		String jsondata = JSON.toJSONString(map);
		

		
		
		
		JSONObject jr=JSONObject.parseObject(jsondata);
		
		if(jr.get("msgDesc")!=null){
			System.out.println(jr.get("msgDesc"));
		}
		else{
			System.out.println("true");
		}
		
		

	}

}
