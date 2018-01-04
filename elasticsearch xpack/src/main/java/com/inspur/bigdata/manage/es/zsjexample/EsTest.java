package com.inspur.bigdata.manage.es.zsjexample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class EsTest {

	public static void main(String[] args) throws IOException {

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

		// 负 后向引用断言
		String regu1 = "(?<!c)a(\\d+)b";
		Pattern p = Pattern.compile(regu1);
		Matcher m = p.matcher("da12bca3434bdca4356bdca234bm");

		if (m.find()) {
			System.out.println(m.group(1));

			System.out.println(m.group(0));

		}

		// 后向引用断言 以及 前向 引用断言
		Pattern p1 = Pattern.compile("(?<=[w]{3}[.]{1})([A-Za-z0-9]+)(?=[.]com)");
		Matcher m1 = p1.matcher("www.wjnb.com");

		if (m1.find()) {
			System.out.println(m1.group(1));
			System.out.println(m1.group(0));
			System.out.println(m1.groupCount());

		}
		t2();

	}

	static void t1() {
		try {
			DefaultHttpClient client = new DefaultHttpClient();

			HttpGet request = new HttpGet("dd");
			HttpResponse response = client.execute(request);

			if (response.getStatusLine().getStatusCode() == 200) {
				String strResult = EntityUtils.toString(response.getEntity());
				String url = URLDecoder.decode("dd", "UTF-8");
			} else {
			}
		} catch (IOException e) {
		} finally {

		}
	}

	static void t2() throws IOException {
		URL url = new URL(
				"http://10.110.13.67/manage-store/service/hive/rest/getTbsByBigSQLUser?clusterId=cluster23eb&userId=wj1-realm23eb&dbName=default");
		URLConnection conn = url.openConnection();
		conn.setDoInput(true);

		BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String str = null;
		while ((str = br.readLine()) != null) {
			System.out.println(str);
		}

	}

}
