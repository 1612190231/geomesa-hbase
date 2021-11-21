/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.quickstart;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.geomesa.example.ShardGeoMesaDataStore;
import org.geomesa.example.data.TestData;
import org.geomesa.example.data.TutorialData;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.hbase.data.HBaseDataStore;
import org.locationtech.geomesa.index.api.WritableFeature;
import org.locationtech.geomesa.index.conf.ColumnGroups;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.locationtech.geomesa.utils.geotools.FeatureUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class GeoMesaInsertWithShard implements Runnable {
    private static final Logger baseInsert = Logger.getLogger("baseInsert");
    private final Map<String, String> params;
    private final TestData data;
    private final boolean cleanup;
    private final boolean readOnly;

    public GeoMesaInsertWithShard(String[] args, Param[] parameters, TestData data) throws ParseException {
        this(args, parameters, data, false);
        baseInsert.info("GeoMesaQuickStart Init...");
    }

    public GeoMesaInsertWithShard(String[] args, Param[] parameters, TestData data, boolean readOnly) throws ParseException {
        // parse the data store parameters from the command line
        baseInsert.info("GeoMesaQuickStart Init start");
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        cleanup = command.hasOption("cleanup");
        this.data = data;
        this.readOnly = readOnly;
        params.forEach((key, value) -> {
            baseInsert.info("key: " + key + "; value: " + value);
        });

//        logger.info(this.data.getTestData());
        baseInsert.info(this.readOnly);
    }

    public Options createOptions(Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);
        if (!readOnly) {
            options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());
        }
        return options;
    }

    @Override
    public void run() {
        HBaseDataStore datastore = null;
        try {
            datastore = createDataStore(params);
//            System.out.println(datastore);

            if (readOnly) {
                ensureSchema(datastore, data);
            } else {
                System.out.println("开始建keys");
                // construct column feature type
                SimpleFeatureType sft = getSimpleFeatureType(data);
                // create schema and reserve metadata
                System.out.println("开始建表");
                datastore.createSchema(sft);
//                System.out.println(sft.getTypeName());
                // get test data
                List<SimpleFeature> features = getTestFeatures(data);
//                writeFeatures(datastore, sft, features);
//                ShardStrategy strategy = new GeoMesaShardStrategy(60);
                writeFeatures2(datastore, sft, features);
            }
        } catch (Exception e) {
            e.printStackTrace();
//            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName(), cleanup);
        }
        System.out.println("Done");
    }

    public HBaseDataStore createDataStore(Map<String, String> params) throws IOException {
        baseInsert.info("加载数据库...");

        // use geotools service loading to get a datastore instance
        HBaseDataStore datastore = (HBaseDataStore) DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        baseInsert.info("加载数据库成功！");
        return datastore;
    }

    public void ensureSchema(DataStore datastore, TutorialData data) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(data.getTypeName());
        if (sft == null) {
            throw new IllegalStateException("Schema '" + data.getTypeName() + "' does not exist. " +
                                            "Please run the associated QuickStart to generate the test data.");
        }
    }

    public SimpleFeatureType getSimpleFeatureType(TestData data) {
        return data.getSimpleFeatureType();
    }

    public List<SimpleFeature> getTestFeatures(TestData data) {
        baseInsert.info("生成测试数据集");
        List<SimpleFeature> features = data.getTestData();
        baseInsert.info("生成测试数据集成功！");
        return features;
    }

    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() > 0) {
            baseInsert.info("Start insert data...");
            double startTime = System.currentTimeMillis();
            // use try-with-resources to ensure the writer is closed
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                         datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation class
                    // and add the USE_PROVIDED_FID hint to the user data
                    ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
//                    logger.info("Hints.USE_PROVIDED_FID is: " + Hints.USE_PROVIDED_FID);

                    // alternatively, you can use the PROVIDED_FID hint directly
                    // toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());

                    // if no feature ID is set, a UUID will be generated for you

                    // make sure to copy the user data, if there is any
                    toWrite.getUserData().putAll(feature.getUserData());

                    // write the feature
                    writer.write();
                }
            }
            double endTime = System.currentTimeMillis();
            baseInsert.info("Insert data success!");
            baseInsert.info("The base insert program running: " + (endTime - startTime)/1000 + "s; Wrote features: " + features.size());
//            baseInsert.info("写入测试数据集成功！");
        }
    }

    public void writeFeatures2(HBaseDataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() > 0) {
            baseInsert.info("Start insert data...");
            double startTime = System.currentTimeMillis();
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                         datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    ShardStart shardStart = new ShardStart();
                    shardStart.startShard(sft, feature);
//                    FeatureUtils.write(writer, feature, true);
                }
            }
            double endTime = System.currentTimeMillis();
            baseInsert.info("Insert data success!");
            baseInsert.info("The base insert program running: " + (endTime - startTime)/1000 + "s; Wrote features: " + features.size());
//            baseInsert.info("写入测试数据集成功！");
        }
    }

    public void cleanup(DataStore datastore, String typeName, boolean cleanup) {
        if (datastore != null) {
            try {
                if (cleanup) {
                    System.out.println("Cleaning up test data");
                    if (datastore instanceof GeoMesaDataStore) {
                        ((GeoMesaDataStore) datastore).delete();
                    } else {
                        ((SimpleFeatureStore) datastore.getFeatureSource(typeName)).removeFeatures(Filter.INCLUDE);
                        datastore.removeSchema(typeName);
                    }
                }
            } catch (Exception e) {
                System.err.println("Exception cleaning up test data: " + e.toString());
            } finally {
                // make sure that we dispose of the datastore when we're done with it
                datastore.dispose();
            }
        }
    }
}
