package com.example.aliyunhbasedemo;

import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import org.apache.hadoop.hbase.client.Connection;

public class QingzhouDAO extends AbstractHBDAO<String, QingzhouRecord> {
    public QingzhouDAO(Connection connection) {
        super(connection);
    }
}
