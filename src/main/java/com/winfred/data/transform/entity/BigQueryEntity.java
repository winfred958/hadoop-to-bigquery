package com.winfred.data.transform.entity;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

/**
 * @author kevin
 * @since 2018/6/29 9:29
 */
public class BigQueryEntity {

    @JSONField(name = "source_database")
    private String sourceDatabase;

    @JSONField(name = "source_table")
    private String sourceTable;

    @JSONField(name = "partition_key")
    private String sourcePartitionKey;

    @JSONField(name = "partition_value")
    private String sourcePartitionValue;

    @JSONField(name = "target_database")
    private String targetDatabase;

    @JSONField(name = "target_table")
    private String targetTable;

    @JSONField(name = "target_date")
    private String targetDate;

    @JSONField(name = "schema_column")
    private String schemaStr;

    @JSONField(name = "tmp_dir")
    private String tmpDir;

    @JSONField(name = "project_id")
    private String projectId;

    private String bucket;

    @JSONField(name = "is_truncate")
    private Boolean is_truncate;

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSourcePartitionKey() {
        return sourcePartitionKey;
    }

    public void setSourcePartitionKey(String sourcePartitionKey) {
        this.sourcePartitionKey = sourcePartitionKey;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    public String getTargetDate() {
        return targetDate;
    }

    public void setTargetDate(String targetDate) {
        this.targetDate = targetDate;
    }

    public String getSchemaStr() {
        return schemaStr;
    }

    public void setSchemaStr(String schemaStr) {
        this.schemaStr = schemaStr;
    }

    public String getTmpDir() {
        return tmpDir;
    }

    public void setTmpDir(String tmpDir) {
        this.tmpDir = tmpDir;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public BigQueryEntity getBigQueryEntity(String str) {
        return JSON.parseObject(str, BigQueryEntity.class);
    }

    public Boolean getIs_truncate() {
        return is_truncate;
    }

    public void setIs_truncate(Boolean is_truncate) {
        this.is_truncate = is_truncate;
    }

    public String getSourcePartitionValue() {
        return sourcePartitionValue;
    }

    public void setSourcePartitionValue(String sourcePartitionValue) {
        this.sourcePartitionValue = sourcePartitionValue;
    }
}
