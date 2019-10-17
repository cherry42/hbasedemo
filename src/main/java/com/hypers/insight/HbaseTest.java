package com.hypers.insight;
import java.io.IOException;
import java.lang.Exception;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
public class HbaseTest {
    public static  Connection connection;
    public static Configuration configuration;
    static {

        configuration = HBaseConfiguration.create();
//        // 设置连接参数：HBase数据库使用的端口
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        // 设置连接参数：HBase数据库所在的主机IP
//        configuration.set("hbase.zookeeper.quorum", "192.168.10.124");
        configuration.addResource("hbase-site.xml");
        try {
            // 取得一个数据库连接对象
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) throws Exception {
//        createTable("insight:bona", "info1","info2");
//        addRowData("insight:bona","1001","info1","name","zhangsan");
//        addRowData("insight:bona","1002","info1","name","lisi");
//        addRowData("insight:bona","1003","info1","name","wangwu");
//        addRowData("insight:bona","1001","info2","course","english");
//        addRowData("insight:bona","1001","info2","hobby","basketball");
//        addRowData("insight:bona","1002","info2","course","math");
//        addRowData("insight:bona","1002","info2","hobby","pingpang");
//        addRowData("insight:bona","1003","info2","course","history");
//        addRowData("insight:bona","1003","info2","hobby","badmiton");
        getAllRows("insight:bona");
//        getRow("insight:bona","1001");
//        getRowQualifier("insight:bona","1001","info1","name");//获取单元格数据
//        deleteMultiRow("insight:bona","1001","1002","1003");
//        truncateTable("insight:bona");
//        dropTable("insight:bona");
    }

    /***
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws Exception
     */
    public static boolean isTableExist(String tableName) throws Exception{
        Admin admin = connection.getAdmin();
        //HTD需要TableName类型的tableName，创建TableName类型的tableName
        //TableName tbName = TableName.valueOf(tableName);
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /***
     * 创建表
     * @param tableName 表名
     * @param cf1 列族名
     * @throws Exception
     */
    public static void createTable(String tableName,String... cf1) throws Exception {
        Admin admin = connection.getAdmin();
        //判断表述否已存在，不存在则创建表
        if(isTableExist(tableName)){
            System.err.println("表" + tableName + "已存在！");
            return;
        }
        //HTD需要TableName类型的tableName，创建TableName类型的tableName
        TableName tbName = TableName.valueOf(tableName);
        //通过HTableDescriptor创建一个HTableDescriptor将表的描述传到createTable参数中
        HTableDescriptor HTD = new HTableDescriptor(tbName);
        //为描述器添加表的详细参数
        for(String cf : cf1){
            // 创建HColumnDescriptor对象添加表的详细的描述
            HColumnDescriptor HCD =new HColumnDescriptor(cf);
            HTD.addFamily(HCD);
        }
        //调用createtable方法创建表
        admin.createTable(HTD);
        System.out.println("创建成功");
    }

    /***
     * 删除表
     * @param tableName
     * @throws Exception
     */
    public static void dropTable(String tableName) throws Exception{
        Admin admin = connection.getAdmin();
        if(isTableExist(tableName)){
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表" + tableName + "删除成功！");
        }else{
            System.out.println("表" + tableName + "不存在！");
        }
    }

    /***
     * 向表中插入数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws Exception
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws Exception{
        //创建HTable对象
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    /***
     * 得到所有数据
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException{
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /***
     * 得到某一行的所有值
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException{
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = hTable.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    /***
     * 获取某一行指定“列族:列”的数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException{
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = hTable.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    /***
     * 删除多行数据
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException{
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
        System.out.println("完成 删除多行");
    }

    public static void truncateTable(String tableName) throws Exception{
        Admin admin = connection.getAdmin();
        if(isTableExist(tableName)){
            admin.disableTable(TableName.valueOf(tableName));
            admin.truncateTable(TableName.valueOf(tableName),true);
            System.out.println("表" + tableName + "清空成功！");
        }else{
            System.out.println("表" + tableName + "不存在！");
        }
    }


}

