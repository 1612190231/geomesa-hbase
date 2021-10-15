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
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class GeoMesaBaseSelect implements Runnable {
    private static Logger baseSelect = Logger.getLogger("baseSelect");
    private final Map<String, String> params;
    private final TutorialData data;
    private final boolean cleanup;
    private final boolean readOnly;

    public GeoMesaBaseSelect(String[] args, Param[] parameters, TutorialData data) throws ParseException {
        this(args, parameters, data, false);
        baseSelect.info("GeoMesaQuickStart Init...");
    }

    public GeoMesaBaseSelect(String[] args, Param[] parameters, TutorialData data, boolean readOnly) throws ParseException {
        // parse the data store parameters from the command line
        baseSelect.info("GeoMesaQuickStart Init start");
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        cleanup = command.hasOption("cleanup");
        this.data = data;
        this.readOnly = readOnly;
        params.forEach((key, value) -> {
            baseSelect.info("key: " + key + "; value: " + value);
        });

//        logger.info(this.data.getTestData());
        baseSelect.info(this.readOnly);
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
            List<Query> queries = getTestQueries(data);
            queryFeatures(datastore, queries);
        } catch (Exception e) {
            e.printStackTrace();
//            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName(), cleanup);
        }
        System.out.println("Done");
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        baseSelect.info("加载数据库...");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        baseSelect.info("加载数据库成功！");
        return datastore;
    }

    public List<Query> getTestQueries(TutorialData data) {
        baseSelect.info("GeoMesaQuickStart.getTestQueries start...");
        return data.getTestQueries();
    }

    public void queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
        for (Query query : queries) {
//            baseSelect.info("Running query " + ECQL.toCQL(query.getFilter()));
//            if (query.getPropertyNames() != null) {
//                baseSelect.info("Returning attributes " + Arrays.asList(query.getPropertyNames()));
//            }
//            if (query.getSortBy() != null) {
//                SortBy sort = query.getSortBy()[0];
//                baseSelect.info("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
//            }
            // submit the query, and get back an iterator over matching features
            // use try-with-resources to ensure the reader is closed
            double startTime = System.currentTimeMillis();
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                double endTime = System.currentTimeMillis();
//                baseSelect.info("数据读取成功！");
                // loop through all results, only print out the first 10
                int n = 0;
                while (reader.hasNext()) {
//                    n++;
                    reader.next();
                    SimpleFeature feature = reader.next();
                    if (n++ < 10) {
                        // use geotools data utilities to get a printable string
                        System.out.println(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                        baseSelect.info(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                    } else if (n == 10) {
                        System.out.println("...");
                        baseSelect.info("...");
                    }
                }
                baseSelect.info("The query: " + ECQL.toCQL(query.getFilter()) + "; run time:" +
                        (endTime - startTime)/1000 + "s; Returned total features: " + n);
            } catch (Exception e) {
//                baseSelect.error("数据读取失败...");
                baseSelect.error("Wrong query " + ECQL.toCQL(query.getFilter()));
//                baseSelect.error(e.fillInStackTrace());
//            throw new RuntimeException("Error running quickstart:", e);
            }
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
