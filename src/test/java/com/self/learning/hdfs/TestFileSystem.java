package com.self.learning.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class TestFileSystem {

    private FileSystem fs ;
    private String uri = "hdfs://server-101:9000/";

    @Before
    public void initConf() {
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(URI.create(uri), conf, "weizhiming");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteFile() throws Exception {
        FSDataOutputStream dos = fs.create(new Path("hdfs://server-101:9000/usr/data/test.txt"));
        FileInputStream fis = new FileInputStream("d:/test.txt");
        IOUtils.copyBytes(fis, dos, 4096);
    }

    @Test
    public void testReadFile() throws Exception {
        FSDataInputStream is = fs.open(new Path("hdfs://server-101:9000/usr/data/test.txt"));
        FileOutputStream fos = new FileOutputStream("d:/test.txt");
        IOUtils.copyBytes(is,fos,4096);
    }

    @Test
    public void testCopyFromLocalFile() throws Exception {
        fs.copyFromLocalFile(new Path("d:/test.txt"),new Path("hdfs://server-101:9000/usr/data/test.txt"));
    }

    @Test
    public void testCopyToLocalFile() throws IOException {
        fs.copyToLocalFile(new Path("hdfs://server-101:9000/usr/data/test.txt"), new Path("d:/test1.txt"));
    }

    @Test
    public void testMakeDir() throws IOException {
        fs.mkdirs(new Path("/usr/data/mydata"));
    }

    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path
                ("hdfs://server-101:9000/usr/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus lfs = iterator.next();
            Path path = lfs.getPath();
            System.out.println(path.getName());
        }
    }
}
