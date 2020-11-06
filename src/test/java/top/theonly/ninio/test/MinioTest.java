package top.theonly.ninio.test;

import io.minio.MinioClient;
import io.minio.ObjectStat;
import io.minio.Result;
import io.minio.errors.*;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import io.minio.messages.Upload;
import io.minio.policy.PolicyType;
import org.junit.Before;
import org.junit.Test;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

/**
 * @ClassName MinioTest
 * @Description: TODO
 * @Author guoshihua
 * @Date 2020/8/17 0017 上午 10:13
 * @Version V1.0
 * @See 版权声明
 **/
public class MinioTest {

    private MinioClient minioClient;

    @Before
    public void init() {
        try {
            minioClient = new MinioClient("http://192.168.221.130:9000", "minioadmin", "minioadmin");
        } catch (MinioException e) {
            System.out.println("minio 服务端链接失败");
        }
    }

    /**
     * 创建一个新的存储桶
     * @throws IOException
     * @throws InvalidKeyException
     * @throws NoSuchAlgorithmException
     * @throws InsufficientDataException
     * @throws ErrorResponseException
     * @throws NoResponseException
     * @throws InvalidBucketNameException
     * @throws XmlPullParserException
     * @throws InternalException
     * @throws RegionConflictException
     */
    @Test
    public void testMakeBucket() throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, ErrorResponseException, NoResponseException, InvalidBucketNameException, XmlPullParserException, InternalException, RegionConflictException {
        boolean exists = minioClient.bucketExists("test"); // 检查存储桶是否存在。
        if (exists) {
            System.out.println("bucket test already exists");
        } else {
            minioClient.makeBucket("test");
            System.out.println("bucket test is created");
        }
    }

    /**
     * 列出所有存储桶。
     * @throws IOException
     * @throws InvalidKeyException
     * @throws NoSuchAlgorithmException
     * @throws InsufficientDataException
     * @throws InternalException
     * @throws NoResponseException
     * @throws InvalidBucketNameException
     * @throws XmlPullParserException
     * @throws ErrorResponseException
     */
    @Test
    public void testListBuckets() throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException {
        List<Bucket> buckets = minioClient.listBuckets();
        System.out.println(buckets);
    }

    /**
     * 列出某个存储桶中的所有对象。
     * @throws XmlPullParserException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws IOException
     */
    @Test
    public void testListObjects() throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try {
            boolean exists = minioClient.bucketExists("asiatrip");
            if (exists) {
                Iterable<Result<Item>> objects = minioClient.listObjects("asiatrip");
                for (Result<Item> result : objects) {
                    Item item = result.get();
                    System.out.println(item.lastModified() + ", " + item.objectSize() + ", " + item.objectName());
                }
            } else {
                System.out.println("bucket asiatrip does not exists");
            }
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }
    }

    /**
     * 列出存储桶中被部分上传的对象。
     * @throws XmlPullParserException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws IOException
     */
    @Test
    public void testListIncompleteUploads() throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try {
            boolean exists = minioClient.bucketExists("asiatrip");
            if (exists) {
                Iterable<Result<Upload>> objects = minioClient.listIncompleteUploads("asiatrip");
                for (Result<Upload> result : objects) {
                    Upload upload = result.get();
                    System.out.println(upload.uploadId() + ", " + upload.objectName());
                }
            } else {
                System.out.println("bucket asiatrip does not exists");
            }
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }
    }

    /**
     * 给一个存储桶+对象前缀设置策略
     * setBucketPolicy(String bucketName, String objectPrefix, PolicyType policy)
     * bucketName	    String	    存储桶名称。
     * objectPrefix	    String	    对象前缀。
     * policy	        PolicyType	要赋予的策略，可选值有[PolicyType.NONE, PolicyType.READ_ONLY, PolicyType.READ_WRITE, PolicyType.WRITE_ONLY].
     * @throws XmlPullParserException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws IOException
     */
    @Test
    public void testSetBucketPolicy() throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try {
            minioClient.setBucketPolicy("test", "downloads", PolicyType.READ_WRITE);
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }
    }

    /**
     * 获得指定对象前缀的存储桶策略。
     * @throws XmlPullParserException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws IOException
     */
    @Test
    public void testGetBucketPolicy() throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try {
            Map<String, PolicyType> map = minioClient.getBucketPolicy("test");
            PolicyType policyType = minioClient.getBucketPolicy("test", "downloads");
            System.out.println("Current policy: " + map);
            System.out.println("Current policy: " + policyType);
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }
    }

    /**
     * 以流的形式下载一个对象。
     * @throws XmlPullParserException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws IOException
     */
    @Test
    public void testGetObject() throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try {
            minioClient.statObject("asiatrip", "网络安全漏洞汇总.doc");
            InputStream in = minioClient.getObject("asiatrip", "网络安全漏洞汇总.doc");
            byte[] bytes = new byte[1024];
            int read;
            while ((read = in.read(bytes, 0, bytes.length)) >= 0) {
                System.out.println(new String(bytes, 0, read));
            }
            in.close();
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }

    }

    /**
     * 下载并将文件保存到本地。
     * @throws XmlPullParserException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws IOException
     */
    @Test
    public void testGetObject2() throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException,
            IOException {
        try {
            minioClient.statObject("asiatrip", "网络安全漏洞汇总.doc");
            minioClient.getObject("asiatrip", "网络安全漏洞汇总.doc", "1.doc");
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }

    }
}
