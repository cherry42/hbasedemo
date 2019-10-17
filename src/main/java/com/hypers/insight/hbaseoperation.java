package com.hypers.insight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.File;


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

public class hbaseoperation {
    public Connection connection;	//connection object
    public  Admin admin;

    public void initconnection() throws Exception
    {
        File workaround = new File(".");
        System.getProperties().put("hadoop.home.dir",workaround.getAbsolutePath());
        new File("./bin").mkdirs();
        try
        {
            new File("./bin/winutils.exe").createNewFile();
        }
        catch (IOException e)
        {
            //
        }
        Configuration conf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

}
