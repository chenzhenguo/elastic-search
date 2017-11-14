package com.inspur.bigdata.manage.es.example;

import java.io.File;
import java.io.PrintWriter;

/**
 * 本地创建不动产单元号，用于造关联数据
 * @author zhaoshengjie
 *
 */
public class CreateBdcdyhFile {

	public static void main(String[] args) {

        int defaultNum = 10000;
        if (args != null && args.length >= 1) {
            defaultNum = Integer.valueOf(args[0]);
        }
        String defaultFileName = "bdcdyhFile";
        // 标记文件生成是否成功
        boolean flag = true;
        try {
//        	String	 qxdm = DataGenUtil.generatorqxdmStr();
//        	String bdcdyh =  DataGenUtil.getBdcdyh(DataGenUtil.generatorqxdmStr());
            // 含文件名的全路径
            String fullPath = "d://" + defaultFileName;
            System.out.println("001:"+fullPath);
            File file = new File(fullPath);
            if (file.exists()) { // 如果已存在,删除旧文件
                System.out.println("002");
                file.delete();
            }
            System.out.println("003");
            file = new File(fullPath);
            System.out.println("004");
            file.createNewFile();
            System.out.println("005");
            // 遍历输出每行
            PrintWriter pfp = new PrintWriter(file);
            for (int i = 1; i <= defaultNum; i++) {
                pfp.print( DataGenUtil.getBdcdyh(DataGenUtil.generatorqxdmStr())+"\n");
            }
            pfp.close();
        } catch (Exception e) {
            flag = false;
            e.printStackTrace();
        }
    

	}

}
