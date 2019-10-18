package com.hypers.insight;

import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/***
 * 实现 触发插入事件
 */
public class HbaseCoprocessorTest extends BaseRegionObserver{

    static Connection connection;
    static Configuration configuration;
    static Table table = null;
    static {
        configuration = HBaseConfiguration.create();
        configuration.addResource("hbase-site.xml");
        try {
            // 取得一个数据库连接对象
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /***
     * 此方法是在put方法调用之前进行调用
     * @param e
     * @param put 是要进行插入的那条数据
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {
        //获取put对象里面的rowkey'zhangsan'
        byte[] row = put.getRow();
        table = connection.getTable(TableName.valueOf("insight:bonaFans"));
        //获取put对象里面的cell
        List<Cell> list = put.get("cf".getBytes(), "star".getBytes());
        Cell cell = list.get(0);
        //创建一个新的put对象
//        put.get
        Put new_put = new Put(subByte(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
        new_put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fensi"), row);
        table.put(new_put);
        connection.close();
    }

    /**
     * 截取byte数组   不改变原数组
     * @param b 原数组
     * @param off 偏差值（索引）
     * @param length 长度
     * @return 截取后的数组
     */
    public static byte[] subByte(byte[] b,int off,int length){
        byte[] b1 = new byte[length];
        System.arraycopy(b, off, b1, 0, length);
        return b1;
    }
}