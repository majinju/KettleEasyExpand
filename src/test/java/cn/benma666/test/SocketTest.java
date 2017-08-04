/**
* Project Name:KettleEasyExpand
* Date:2017年8月4日
* Copyright (c) 2017, jingma All Rights Reserved.
*/

package cn.benma666.test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
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
        ServerSocket ss = new ServerSocket(889);
        Socket socket = null;
        while(true){
            socket = ss.accept();
            Scanner sc = new Scanner(socket.getInputStream());
            while(sc.hasNext()){
                System.out.println(sc.next());
//                System.out.println(new String(sc.next().getBytes("UTF-8"),"UTF-8"));
            }
            sc.close();
        }
    }
}
