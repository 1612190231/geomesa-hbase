package org.geomesa.example.hbase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.CommandLineDataStore;
import org.geomesa.example.quickstart.GeoMesaInsertWithShardTest;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author luchengkai
 * @description 分片测试类
 * @date 2021/10/26 11:23
 */
public class InsertWithShardTest extends GeoMesaInsertWithShardTest {
    private static Logger baseInsert = Logger.getLogger("baseInsert");

    // uses gdelt data
    public InsertWithShardTest(String[] args) throws ParseException {
        super(args, new HBaseDataStoreFactory().getParametersInfo(), new GDELTData());
        baseInsert.info("InsertWithShardTest Init...");
    }

    public static void main(String[] args) {
        try {
            new InsertWithShardTest(args).run();
        } catch (ParseException e) {
//            logger.error(e.printStackTrace());
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
