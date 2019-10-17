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

import java.io.IOException;

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
        //获取put对象里面的rowkey'ergouzi'
        byte[] row = put.getRow();
        table = connection.getTable(TableName.valueOf("fans"));
        //获取put对象里面的cell
        List<Cell> list = put.get("cf".getBytes(), "star".getBytes());
        Cell cell = list.get(0);
        //创建一个新的put对象
        Put new_put = new Put(cell.getValueArray());
        new_put.addColumn("cf".getBytes(), "fensi".getBytes(), row);
        table.put(new_put);
        connection.close();
    }
}