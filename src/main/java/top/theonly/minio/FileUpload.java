package top.theonly.minio;

import io.minio.MinioClient;
import io.minio.errors.*;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * @ClassName FileUpload
 * @Description: TODO
 * @Author guoshihua
 * @Date 2020/8/17 0017 上午 9:48
 * @Version V1.0
 * @See 版权声明
 **/
public class FileUpload {

    public static void main(String[] args) throws NoSuchAlgorithmException, IOException, InvalidKeyException, XmlPullParserException {

        try {
            MinioClient minioClient = new MinioClient("http://192.168.221.130:9000", "minioadmin", "minioadmin");
            boolean isExists = minioClient.bucketExists("asiatrip");
            if (isExists) {
                System.out.println("Bucket already exists...");
            } else {
                minioClient.makeBucket("asiatrip");
            }
            minioClient.putObject("asiatrip", "网络安全漏洞汇总.doc", "C:\\Users\\Administrator\\Downloads\\网络安全漏洞汇总.doc");
            System.out.println("file is successfully uploaded as 网络安全漏洞汇总.doc to `asiatrip` bucket.");
        } catch (MinioException e) {
            e.printStackTrace();
        }

    }
}
