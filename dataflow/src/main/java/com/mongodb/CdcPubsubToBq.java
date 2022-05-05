package com.mongodb;


import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.LinkedHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Helpers.Functions;
import com.mongodb.Process.ConvertJSONtoTableRow;
import com.mongodb.Options.ParameterOptionsCDC;

public class CdcPubsubToBq {
    
    private static final Logger LOG = LoggerFactory.getLogger(CdcPubsubToBq.class);

    public static void main(String[] args) throws Exception {

        String propFileName = "configuration.properties";
        Functions.GetPropertyValues objProperty = new Functions.GetPropertyValues();
        String projectId = objProperty.getPropValues("gcp_sm_project_id", propFileName); 
        String mongodbName = objProperty.getPropValues("gcp_mongodb_name", propFileName);

        Storage storage = StorageOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();

        ParameterOptionsCDC options = PipelineOptionsFactory.fromArgs(args).as(ParameterOptionsCDC.class);

        String appname = objProperty.getPropValues("analytical_bup.app_name",propFileName) + "-"+options.getSchema();
        options.setAppName(appname);
        LOG.info("La AppName es:" + appname);
        options.setProject(objProperty.getPropValues("gcp_sm_project_id", propFileName));
        options.setTempLocation(objProperty.getPropValues("gcp_temp_location",propFileName));
        options.setStagingLocation(objProperty.getPropValues("gcp_staging_location",propFileName));
        options.setRegion(objProperty.getPropValues("gcp_region",propFileName));
        //options.setNetwork(objProperty.getPropValues("gcp_network",propFileName));
        //options.setSubnetwork(objProperty.getPropValues("gcp_subnetwork",propFileName));
        options.setStreaming(true);
        options.setEnableStreamingEngine(true);
        options.setMaxNumWorkers(Integer.parseInt(objProperty.getPropValues("analytical_bup.max_workers",propFileName)));

        Pipeline pipeline = Pipeline.create(options);

        String schema = options.getSchema().toLowerCase();
        //String databaseNameBup = objProperty.getPropValues("gcp_sm_jdbc_database_bup",propFileName);
        String topicsPrefix = objProperty.getPropValues("analytical_bup.topics_prefix", propFileName);
        String subscriptionsPrefix = objProperty.getPropValues("analytical_bup.subscriptions_prefix", propFileName);
        String bucket_url = objProperty.getPropValues("analytical_bup.bucket_url", propFileName);
        String[] tableNames = objProperty.getPropValues("analytical_bup.table_names_"+schema, propFileName).split(",");
        String bucket_schema_root_path =objProperty.getPropValues("analytical_bup.bucket_schema_root_path_"+schema, propFileName);
        String PrefixDatasetName = objProperty.getPropValues("analytical_bup.prefix_bq_dataset", propFileName);
        LinkedHashMap<String, PCollection<String>> hashTopicPcol = new LinkedHashMap<>();
        LinkedHashMap<String, String> hashSchema = new LinkedHashMap<>();

        
        for (String fullTableName : tableNames) {
            String[] collectionNameParts = fullTableName.split("\\.");
            String topic = String.format("%s.%s", mongodbName, fullTableName);
            String subscription = String.format("%s.%s-sub", mongodbName, fullTableName);
            LOG.info("bucket_url :" + bucket_url);
            LOG.info("bucket_schema_root_path :" + bucket_schema_root_path);
            Blob blob = storage.get(bucket_url, bucket_schema_root_path + "schema_"+ collectionNameParts[1] +".json");
            String fileContent = new String(blob.getContent());
            //LOG.info("fileContent: " + fileContent );
            hashSchema.put(topic, fileContent);
            LOG.info("subscription: " + subscriptionsPrefix + subscription);
            PCollection<String> message = pipeline.apply("Read PubSub Messages from " + subscription,
            PubsubIO.readStrings().fromSubscription(subscriptionsPrefix + subscription));        
            hashTopicPcol.put(topic, message);
        }
        
        //System.out.println("hashTopicPcol:"+hashTopicPcol);
        //System.out.println("hashSchema:"+hashSchema);

        
        for (String topicHashKey : hashTopicPcol.keySet()) {
            int first = topicHashKey.indexOf(".");
            int second = topicHashKey.indexOf(".", first + 1);
            String bqDatasetName = PrefixDatasetName+topicHashKey.substring(first+1,second);
            String bqTable = topicHashKey.substring(second + 1) + "_delta";
            LOG.info("bqTable: " + bqTable);
            //if( bqTable == "master_person_delta"){
            //    bqDatasetName = "ent_dev_bup_analytics_identyticore";
            //}
            LOG.info("bqDatasetName: " + bqDatasetName);
            String bqSchema = hashSchema.get(topicHashKey);
            
            hashTopicPcol.get(topicHashKey).apply("Convert messages to TableRows " + topicHashKey, ParDo.of(new ConvertJSONtoTableRow()))
                                               .apply("Write to BigQuery table " + bqTable , BigQueryIO.writeTableRows()
                                                    .to(String.format("%s:%s.%s", projectId, bqDatasetName, bqTable))
                                                    .withJsonSchema(bqSchema)
                                                    //.withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                                                    .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                                    //.withTriggeringFrequency(Duration.standardSeconds(30))
                                                    .withCustomGcsTempLocation(options.getTempGCSBQBucket())
                                                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                                    //.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                                    .withoutValidation());
            
            
        }

        pipeline.run().waitUntilFinish();
    }
}
