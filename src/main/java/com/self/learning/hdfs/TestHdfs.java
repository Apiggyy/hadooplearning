package com.self.learning.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class TestHdfs {

    public static void main(String[] args) throws Exception {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        String url = "hdfs://server-101:9000/usr/data/hello.txt";
        URL u = new URL(url);
        URLConnection conn = u.openConnection();
        InputStream is = conn.getInputStream();
        FileOutputStream fos = new FileOutputStream("d:/hello.txt");
        byte[] buf = new byte[1024];
        int len = -1;
        while ((len = is.read(buf)) != -1) {
            fos.write(buf);
        }
        is.close();
        fos.close();
        System.out.println("over");
    }
}
