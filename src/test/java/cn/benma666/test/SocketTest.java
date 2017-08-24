/**
* Project Name:KettleEasyExpand
* Date:2017年8月4日
* Copyright (c) 2017, jingma All Rights Reserved.
*/

package cn.benma666.test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Scanner;

import org.junit.Test;

/**
 *  <br/>
 * date: 2017年8月4日 <br/>
 * @author jingma
 * @version 
 */
public class SocketTest {

    @Test
    public void serverSocketTest() throws IOException{
        @SuppressWarnings("resource")
        ServerSocket ss = new ServerSocket(991);
        Socket socket = null;
        while(true){
            socket = ss.accept();
            Scanner sc = new Scanner(socket.getInputStream());
            while(sc.hasNext()){
                System.out.println(sc.next());
            }
            sc.close();
        }
    }
    
    @Test
    public void kkjxTest(){
        kkjx();
    }

    public void kkjx() {
        int MsgBeginFlag = 0x77aa77aa;
        int MsgEndFlag = 0x77ab77ab;
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(991);
            Socket socket = null;
            while(true){
                socket = ss.accept();
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                byte [] temp = new byte[4];
                while(true){
                    dis.read(temp, 0, temp.length);
                    if(bytestoint(temp)==MsgBeginFlag){
                        break;
                    }
                }
                dis.read(temp,0 , temp.length);
                int msgCount = bytestoint(temp);
                System.out.println("消息长度："+msgCount);
                dis.read(temp,0 , temp.length);
                int editid = bytestoint(temp);
                msgCount -= 4;
                System.out.println("版本号："+editid);
                dis.read(temp,0 , temp.length);
                int cmdid = bytestoint(temp);
                msgCount -= 4;
                System.out.println("命令码："+cmdid);
                dis.read(temp,0 , temp.length);
                int xmllen = bytestoint(temp);
                msgCount -= 4;
                System.out.println("xml长度："+xmllen);
                byte[] xmlBytes = new byte[xmllen];
                dis.read(xmlBytes,0 , xmlBytes.length);
                msgCount -= xmlBytes.length;
                System.out.println(new String(xmlBytes,"UTF-8"));
                dis.read(temp,0 , temp.length);
                int imagenum = bytestoint(temp);
                msgCount -= 4;
                System.out.println("图片数："+imagenum);
                for(int i=0;i<imagenum;i++){
                    dis.read(temp,0 , temp.length);
                    int imgSize = bytestoint(temp);
                    msgCount -= 4;
                    System.out.println(i+"图片大小："+imgSize);
                    File file = new File("/tmp/kk"+i+".jpg");
                    FileOutputStream fo = new FileOutputStream(file);
                    byte[] imgBytes = new byte[imgSize];
                    int j = 0;
                    int step = 1024*10;
                    while(j<imgSize){
                        if(imgSize-j<step){
                            step = imgSize-j;
                        }
                        dis.read(imgBytes, j, step);
                        j = j+step;
                    }
                    fo.write(imgBytes);
                    fo.close();
                }
                dis.read(temp, 0, temp.length);
                
                if(bytestoint(temp)==MsgEndFlag){
                    System.out.println("ok");
                }else{
                    System.out.println(new String(temp,"UTF-8"));
                    System.out.println("error");
                }
                dis.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if(ss!=null){
                try {
                    ss.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
//            kkjx();
        }
    }
    @Test
    public void intBytesTest(){
        int number = 23534645;
        byte[] bs = inttobytes(number);
        System.out.println(Arrays.toString(bs));
        System.out.println(bytestoint(bs));
    }
    /**
     * btye数组转int
     * @param b
     * @return
     */
    public static int bytestoint(byte[] b) {
        int value = 0;
        for(int i=0;i<4;i++){
            int shift = (4-1-i)*8;
            value += (b[i+0]&0x000000FF)<<shift;
        }
        return value;
    }
    /**
     * @param number
     * @return
     */
    public static byte[] inttobytes(int number) {
        byte[] bytes = new byte[4];  
        bytes[0] = (byte) (number >>> 24);  
        bytes[1] = (byte) (number >>> 16);  
        bytes[2] = (byte) (number >>> 8);  
        bytes[3] = (byte) number;
        return bytes;
    }
}
