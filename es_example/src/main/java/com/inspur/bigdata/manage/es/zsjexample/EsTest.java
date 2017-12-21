package com.inspur.bigdata.manage.es.zsjexample;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.client.transport.TransportClient;

public class EsTest {

	public static void main(String[] args) {

		String str = "hdfs://idapcluster/apps/hive/warehousr";

		System.out.println(str.replaceAll("[a-zA-Z]+://[a-zA-Z]+", ""));

	}

}
