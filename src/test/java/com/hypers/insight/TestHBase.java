package com.hypers.insight;

import org.junit.Test;

public class TestHBase {
    @Test
    public void put(){
//        //创建conf对象    会加载你项目资源文件下的两个XML文件
////        Configuration conf = HBaseConfiguration.create();
////        //通过连接工厂创建连接对象
////        Connection conn = ConnectionFactory.createConnection(conf);
////        //通过连接查询tableName对象    表名
////        TableName tname = TableName.valueOf("ns1:t1");
////        //获得table
////        Table table = conn.getTable(tname);
////        //通过bytes工具类创建字节数组(将字符串)
////        byte[] rowid = Bytes.toBytes("row1");  //rowkey名
////        //创建put对象
////        Put put = new Put(rowid);
////        byte[] f1 = Bytes.toBytes("f1");  //列簇名
////        byte[] id = Bytes.toBytes("id") ; //列名
////        byte[] value = Bytes.toBytes(102); //设置值
////        put.addColumn(f1,id,value);
////        //执行插入
////        table.put(put);
    }
}
