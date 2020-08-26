package com.zzc;


import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.*;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;


public class TestDouble {

    //文件输入输出的路径
    //private static final String FILEPATH = "/home/liukai/tagsComputeTest/0803/";
    //private static final String FILEPATH = "/Users/liukai/Desktop/";

    public static final BigDecimal ONE_DAY_MILLIS = new BigDecimal(24*3600*1000);


    public static Map<String,Integer> map=new HashMap<String,Integer>();

    public static void main(String[] args) {

        //读取的文件名称
        String[] filenames = args[1].split(",");
        //输出的文件名称
        String resultPath = args[0];

        readFiles(resultPath,filenames);


//        String phyFile = /*FILEPATH + */filename;
//
//        String outFile = /*FILEPATH + */resultPath;
//        System.out.println(phyFile);
//
//        readFile(phyFile, outFile);

        //readHDFSFile(filename);



    }



    public static void readFiles(String output,String ...files){


        for (String file : files) {

            FileInputStream fis = null;
            InputStreamReader inputStreamReader =  null;

            BufferedReader br = null;
            try {
                fis = new FileInputStream(file);
                inputStreamReader = new InputStreamReader(fis);

                br = new BufferedReader(inputStreamReader);
                String line = "";
                int times=0;
                while ((line=br.readLine())!=null){

                    if(map.get(line)!=null){
                        times=map.get(line)+1;
                        map.put(line,times);
                    }else{
                        map.put(line,1);
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        for (Map.Entry<String, Integer> entry : map.entrySet()) {

            if(entry.getValue()!=2){
                System.out.println(entry.getKey()+","+entry.getValue());
                writefile(entry.getKey()+","+entry.getValue(),output);
            }
        }

    }


    public static void readHDFSFile(String pathStr) {
        //url流处理器工程
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://10.0.17.32:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(conf);

            Path path = new Path(pathStr);

            FSDataInputStream fsDataInputStream = fileSystem.open(path);

            BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String line = "";

            while ((line = br.readLine()) != null) {

                System.out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    public static void readFile(String filePath,String resultPath){
        // 读取文件每次一行
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null; //用于包装InputStreamReader,提高处理性能。因为BufferedReader有缓冲的，而InputStreamReader没有。
        try {
            String line = "";

            // 读取文件
            fis = new FileInputStream(filePath);// FileInputStream

            // 从文件系统中的某个文件中获取字节
            isr = new InputStreamReader(fis);// InputStreamReader 是字节流通向字符流的桥梁,
            br = new BufferedReader(isr);// 从字符输入流中读取文件中的内容,封装了一个new InputStreamReader的对象

            while ((line = br.readLine()) != null) {

                String daysGap="-1";

                String[] columns = line.split(",");

                String fieldScope = columns[0];

                String[] values = columns[1].split("\\|");

//                if(values.length==2){
//
//                    daysGap=values[0];
//                } else {
//                    daysGap=columns[1];
//                }

                String s=columns[0].replace("\"", "")+"\t"+columns[1].replace("\"", "")+"\t"+columns[2].replace("\"", "")+"\t"+columns[3].replace("\"", "");
                writefile(s,resultPath);

            }

        } catch (FileNotFoundException e) {
            System.out.println("找不到指定文件");
        } catch (IOException e) {
            System.out.println("读取文件失败");
        } catch (NumberFormatException e){
            e.printStackTrace();
        } finally {
            try {
                br.close();
                isr.close();
                fis.close();
                // 关闭的时候最好按照先后顺序关闭最后开的先关闭所以先关s,再关n,最后关m
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 将数据写入文件
     */
    public static void writefile (String output,String fileName){
        FileOutputStream outSTr = null;
        BufferedOutputStream Buff = null;
        try {
            outSTr = new FileOutputStream(new File(fileName),true);
            Buff = new BufferedOutputStream(outSTr);
            Buff.write(output.getBytes());
            Buff.write("\r\n".getBytes());
            Buff.flush();
            Buff.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                outSTr.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
