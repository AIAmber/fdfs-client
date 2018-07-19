package cn.com.sparknet.monitor;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.csource.common.MyException;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;

import org.apache.commons.io.IOUtils;

import java.util.UUID;

import org.junit.After;
import org.junit.Test;
import org.junit.Before;

public class FdfsUpload {
    public String conf_filename = "E:\\Java\\fdfs\\sparknet-fastdfs\\src\\main\\resources\\fdfs_client.conf"; // pwd of conf

    public String local_filename = "E:\\Python\\image\\hi\\Jin.jpg"; //pwd of files

    @Before
    public void setUp() throws Exception{}

    @After
    public void tearDown() throws Exception{}

    @Test
    public void testUpload() {
        try{
            ClientGlobal.init(conf_filename);

            TrackerClient trackerClient = new TrackerClient();
            TrackerServer trackerServer = trackerClient.getConnection();
            StorageServer storageServer = null;
//            StorageClient1 storageClient1 = new StorageClient1(trackerServer, storageServer);

            StorageClient storageClient = new StorageClient(trackerServer, storageServer);

            NameValuePair nameValuePair[] = new NameValuePair[]{
                    new NameValuePair("AAA", "aaa"),
                    new NameValuePair("BBB", "bbb")
            };
            String fileIds[] = storageClient.upload_file(local_filename, "jpg", nameValuePair);

            System.out.println(fileIds.length);
            System.out.println("Group:" + fileIds[0]);
            System.out.println("Path:" + fileIds[1]);
            System.out.println("URL: http://116.196.111.122/" + fileIds[0] + "/" + fileIds[1]);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MyException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDownload() {
        try {

            ClientGlobal.init(conf_filename);

            TrackerClient trackerClient = new TrackerClient();
            TrackerServer trackerServer = trackerClient.getConnection();
            StorageServer storageServer = null;

            StorageClient storageClient = new StorageClient(trackerServer, storageServer);
//            byte[] b = storageClient.download_file("group1", "M00/00/00/wKgRcFV_08OAK_KCAAAA5fm_sy874.conf");
            byte[] b = storageClient.download_file("group2", "M00/00/00/rBCKO1tQtoeAZXblAABNnVpuSyI889.jpg");
            System.out.println(b);
            IOUtils.write(b, new FileOutputStream("E:/"+UUID.randomUUID().toString()+".jpg"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetFileInfo(){
        try {
            ClientGlobal.init(conf_filename);

            TrackerClient trackerClient = new TrackerClient();
            TrackerServer trackerServer = trackerClient.getConnection();
            StorageServer storageServer = null;

            StorageClient storageClient = new StorageClient(trackerServer, storageServer);
//            FileInfo fi = storageClient.get_file_info("group1", "M00/00/00/wKgRcFV_08OAK_KCAAAA5fm_sy874.conf");
            FileInfo fileInfo = storageClient.get_file_info("group2", "M00/00/00/rBCKO1tQtoeAZXblAABNnVpuSyI889.jpg");
            System.out.println(fileInfo.getSourceIpAddr());
            System.out.println(fileInfo.getFileSize());
            System.out.println(fileInfo.getCreateTimestamp());
            System.out.println(fileInfo.getCrc32());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetFileMate(){
        try {
            ClientGlobal.init(conf_filename);

            TrackerClient trackerClient = new TrackerClient();
            TrackerServer trackerServer = trackerClient.getConnection();
            StorageServer storageServer = null;

            StorageClient storageClient = new StorageClient(trackerServer,
                    storageServer);
//            NameValuePair nvps [] = storageClient.get_metadata("group1", "M00/00/00/wKgRcFV_08OAK_KCAAAA5fm_sy874.conf");
            NameValuePair nameValuePairs [] = storageClient.get_metadata("group2", "M00/00/00/rBCKO1tQtoeAZXblAABNnVpuSyI889.jpg");
            for(NameValuePair nvp : nameValuePairs){
                System.out.println(nvp.getName() + ":" + nvp.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    @Test
//    public void testDelete(){
//        try {
//            ClientGlobal.init(conf_filename);
//
//            TrackerClient trackerClient = new TrackerClient();
//            TrackerServer trackerServer = trackerClient.getConnection();
//            StorageServer storageServer = null;
//
//            StorageClient storageClient = new StorageClient(trackerServer,
//                    storageServer);
////            int i = storageClient.delete_file("group1", "M00/00/00/wKgRcFV_08OAK_KCAAAA5fm_sy874.conf");
//            int i = storageClient.delete_file("group2", "M00/00/00/rBCKO1tQtoeAZXblAABNnVpuSyI889.jpg");
//            System.out.println( i==0 ? "删除成功" : "删除失败:"+i);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
