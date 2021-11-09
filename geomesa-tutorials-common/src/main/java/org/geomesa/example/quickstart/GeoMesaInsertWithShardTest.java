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
import org.geomesa.example.data.TutorialData;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class GeoMesaInsertWithShardTest implements Runnable {
    private static Logger baseInsert = Logger.getLogger("baseInsert");
    private final Map<String, String> params;
    private final TutorialData data;
    private final boolean cleanup;
    private final boolean readOnly;

    public GeoMesaInsertWithShardTest(String[] args, Param[] parameters, TutorialData data) throws ParseException {
        this(args, parameters, data, false);
        baseInsert.info("GeoMesaQuickStart Init...");
    }

    public GeoMesaInsertWithShardTest(String[] args, Param[] parameters, TutorialData data, boolean readOnly) throws ParseException {
        // parse the data store parameters from the command line
        baseInsert.info("GeoMesaInsertWithShardTest Init start");
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
        initializeFromOptions(command);
    }

    public Options createOptions(Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);
        if (!readOnly) {
            options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());
        }
        return options;
    }

    public void initializeFromOptions(CommandLine command) {
    }

    @Override
    public void run() {
        DataStore datastore = null;
        try {
            datastore = createDataStore(params);

            if (readOnly) {
                ensureSchema(datastore, data);
            } else {
                SimpleFeatureType sft = getSimpleFeatureType(data);
                createSchema(datastore, sft);
                List<SimpleFeature> features = getTestFeatures(data);
                writeFeatures(datastore, sft, features);
            }
        } catch (Exception e) {
            e.printStackTrace();
//            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName(), cleanup);
        }
        System.out.println("Done");
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        baseInsert.info("加载数据库...");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
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

    public SimpleFeatureType getSimpleFeatureType(TutorialData data) {
        return data.getSimpleFeatureType();
    }

    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        baseInsert.info("创建schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
        datastore.createSchema(sft);
//        System.out.println("创建schema成功！");
//        System.out.println();
        baseInsert.info("创建schema成功！");
    }

    public List<SimpleFeature> getTestFeatures(TutorialData data) {
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
