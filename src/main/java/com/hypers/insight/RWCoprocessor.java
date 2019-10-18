package com.hypers.insight;

import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
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
 * 实现 读写分离 A列只写，删  B列只读
 * 表名：insight:bonaRW
 * 族 F  族下列 A, B
 */
public class RWCoprocessor extends BaseRegionObserver {

//    private static final Log LOG = LogFactory.getLog(MyRegionObserver.class);

    static Connection connection;
    static Configuration configuration;
    // 设定只有F族下的列才能被操作，且A列只写，B列只读。的语言
    private static final String FAMAILLY_NAME = "F";
    private static final String ONLY_PUT_COL = "A";
    private static final String ONLY_READ_COL = "B";
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

    /**
     * 需求 1.不允许插入B列 2.只能插入A列 3.插入的数据必须为整数 4.插入A列的时候自动插入B列
     */
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put put, final WALEdit edit,
                       final Durability durability) throws IOException {
        // 首先查看单个put中是否有对只读列有写操作
        List<Cell> cells = put.get(Bytes.toBytes(FAMAILLY_NAME),
                Bytes.toBytes(ONLY_READ_COL));
        if (cells != null && cells.size() != 0) {
            throw new IOException("User is not allowed to write read_only col.");
        }
        // 检查A列
        cells = put.get(Bytes.toBytes(FAMAILLY_NAME),
                Bytes.toBytes(ONLY_PUT_COL));
        if (cells == null || cells.size() == 0) {
            // 当不存在对于A列的操作的时候则不做任何的处理，直接放行即可
            return;
        }
        // 当A列存在的情况下在进行值得检查，查看是否插入了整数
        byte[] aValue = null;
        for (Cell cell : cells) {
            try {
                aValue = CellUtil.cloneValue(cell);
                Integer.valueOf(Bytes.toString(aValue));
            } catch (Exception e1) {
                throw new IOException("Can not put an un_number value to A col.");
            }
        }
        put.addColumn(Bytes.toBytes(FAMAILLY_NAME), Bytes.toBytes(ONLY_READ_COL), aValue);//指定族，列，值
    }

    /**
     * 需求 1.不能删除B列 2.只能删除A列 3.删除A列的时候需要一并删除B列
     */
    @Override
    public void preDelete(
            final ObserverContext<RegionCoprocessorEnvironment> e,
            final Delete delete, final WALEdit edit, final Durability durability)
            throws IOException {
        // 首先查看是否对于B列进行了指定删除
        List<Cell> cells = delete.getFamilyCellMap().get(Bytes.toBytes(FAMAILLY_NAME));
        if (cells == null || cells.size() == 0) {
            // 如果客户端没有针对于FAMAILLY_NAME列族的操作则不用关心，让其继续操作即可。
            return;
        }
        // 开始检查F列族内的操作情况
        byte[] qualifierName = null;
        boolean aDeleteFlg = false;
        for (Cell cell : cells) {
            qualifierName = CellUtil.cloneQualifier(cell);

            // 检查是否对B列进行了删除，这个是不允许的
            if (Arrays.equals(qualifierName, Bytes.toBytes(ONLY_READ_COL))) {
                //LOG.info("Can not delete read only B col.");
                throw new IOException("Can not delete read only B col.");
            }

            // 检查是否存在对于A队列的删除
            if (Arrays.equals(qualifierName, Bytes.toBytes(ONLY_PUT_COL))) {
                //LOG.info("there is A col in delete operation!");
                aDeleteFlg = true;
            }
        }
        // 如果对于A列有删除，则需要对B列也要删除
        if (aDeleteFlg)
        {
            delete.addColumn(Bytes.toBytes(FAMAILLY_NAME), Bytes.toBytes(ONLY_READ_COL));
        }

    }


}
