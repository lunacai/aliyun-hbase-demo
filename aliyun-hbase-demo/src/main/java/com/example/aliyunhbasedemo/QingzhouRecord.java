package com.example.aliyunhbasedemo;

import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBTable;

@HBTable(name = "testCanace", families = {@Family(name = "family")})
public class QingzhouRecord implements HBRecord<String> {

    private String key;

    @HBColumn(family = "family", column = "content")
    private String content;

    public String composeRowKey() {
        return key;
    }

    public void parseRowKey(String rowKey) {
        this.key = rowKey;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
