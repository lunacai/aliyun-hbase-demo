package com.example.aliyunhbasedemo.controller;

import com.example.aliyunhbasedemo.QingzhouDAO;
import com.example.aliyunhbasedemo.QingzhouRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author angju
 * @date 2021/1/18 16:38
 */
@RestController()
@RequestMapping("/api/aliyun/hbase")
public class AliyunHbaseController {
    static Map<String, String> hashMap = new HashMap<>();

    static Connection connection;

    static {
        // 新建一个Configuration
        Configuration conf = HBaseConfiguration.create();
        //  集群的连接地址(公网地址)在控制台页面的数据库连接界面获得
        conf.set("hbase.zookeeper.quorum", "");
        // xml_template.comment.hbaseue.username_password.default
        conf.set("hbase.client.username", "");
        conf.set("hbase.client.password", "");
        // 如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
        //conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
        // 如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
        //conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
        //conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
    }

    private QingzhouDAO qingzhouDAO = new QingzhouDAO(connection);

    @GetMapping("/createtable")
    public String createTable(@RequestParam String tablename) {
        try (Admin admin = connection.getAdmin()) {
            // 建表
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tablename));
            htd.addFamily(new HColumnDescriptor(Bytes.toBytes("family")));
            // 创建一个只有一个分区的表
            // 在生产上建表时建议根据数据特点预先分区
            admin.createTable(htd);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return "OK";
    }


    @GetMapping("/get")
    public String get(@RequestParam String content) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf("testCanace"))) {
            // 单行读取
            Get get = new Get(Bytes.toBytes(content));
            Result res = table.get(get);
            System.out.println(res);
        }
        if (qingzhouDAO.get(content) == null) {
            return "no";
        }
        return qingzhouDAO.get(content).getKey() + "," + qingzhouDAO.get(content).getContent();
    }

    @GetMapping("/insert")
    public String insert(@RequestParam String content) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf("testCanace"))) {
            // 插入数据
            Put put = new Put(Bytes.toBytes("row"));
            put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("content"), Bytes.toBytes("value" + System.currentTimeMillis()));
            table.put(put);
        }
        QingzhouRecord qingzhouRecord = new QingzhouRecord();
        qingzhouRecord.setKey("a");
        qingzhouRecord.setContent(content);
        qingzhouDAO.persist(qingzhouRecord);
        return "ok";
    }


    @GetMapping("/delete")
    public String delete(@RequestParam String content) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf("testCanace"))) {
            // 删除一行数据
            Delete delete = new Delete(Bytes.toBytes(content));
            table.delete(delete);
        }
        return "ok";
    }

    @GetMapping("/deleteTable")
    public String deleteTable(@RequestParam String tablename) throws IOException {
        try (Admin table = connection.getAdmin()) {
            table.disableTable(TableName.valueOf(tablename));
            table.deleteTable(TableName.valueOf(tablename));
        }
        return "ok";
    }
}
