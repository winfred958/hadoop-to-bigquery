package com.winfred.data.transform;/**
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @author kevin.hu
 * @date 2018/6/2613:34
 */

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author kevin
 * @since 2018/6/26 13:34
 */
public class BigQueryConfigHandler {

    public static Configuration getOutputConfig(Configuration hadoopConfiguration) throws IOException {

        String databaseName = "";
        String tableName = "";

        String fullyQualifiedInputTableId = "publicdata:samples.shakespeare";
        String projectId = "bi-advice-204600";
        String bucket = "staging.bi-advice-204600.appspot.com";

        // Input configuration.
        hadoopConfiguration.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);
        hadoopConfiguration.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket);

        BigQueryConfiguration.configureBigQueryInput(hadoopConfiguration, fullyQualifiedInputTableId);

        //    BigQueryConfiguration.configureBigQueryOutput()

        // Output parameters.
        String outputTableId = projectId + ":" + databaseName + "." + tableName;
        // Temp output bucket that is deleted upon completion of job.
        String outputGcsPath = ("gs://" + bucket + "/test/tmp/${databaseName}/${tableName}");

        TableSchema outputTableSchemaJson = null;
        // Output configuration.
        // Let BigQuery auto-detect output schema (set to null below).

        BigQueryOutputConfiguration.configure(
                hadoopConfiguration,
                outputTableId,
                outputTableSchemaJson,
                outputGcsPath,
                BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
                TextOutputFormat.class
        );

        hadoopConfiguration.set("mapreduce.job.outputformat.class", IndirectBigQueryOutputFormat.class.getName());

        // Truncate the table before writing output to allow multiple runs.
        hadoopConfiguration.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY, "WRITE_TRUNCATE");
        return hadoopConfiguration;
    }
}
