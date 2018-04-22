package com.self.learning.hdfs;

import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TestData {

    public static void main(String[] args) throws IOException {
        //FileInputStream fis = new FileInputStream
        //        ("E:\\学习资料\\学习视频\\黑马12期大数据\\hadoop课程中用到的测试数据\\data\\HTTP_20130313143750.dat");
        //byte[] buffer = new byte[1024];
        //int len = -1;
        //while ((len = fis.read(buffer)) != -1) {
        //    String content = new String(buffer, 0, len);
        //    System.out.println(content);
        //}

        FileReader fr = new FileReader("E:\\\\学习资料\\\\学习视频\\\\黑马12期大数据\\\\hadoop课程中用到的测试数据\\\\data" +
                "\\\\HTTP_20130313143750.dat");
        BufferedReader br = new BufferedReader(fr);
        while (br.read() != -1) {
            String line = br.readLine();
            String[] fields = StringUtils.split(line, "\t");
            //for (int i = 0; i < fields.length; i++) {
            //    System.out.print("fields[" + i + "]" + fields[i] + "\t");
            //}
            System.out.println("msisdn: " + fields[1] + "\tupflow: " + fields[7] + "\tdownflow: " + fields[8]);
            //System.out.println(line);
        }
    }
}
