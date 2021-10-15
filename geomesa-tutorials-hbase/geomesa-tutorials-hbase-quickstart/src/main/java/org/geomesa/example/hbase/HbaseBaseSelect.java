/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.hbase;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.quickstart.GeoMesaBaseSelect;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;

public class HbaseBaseSelect extends GeoMesaBaseSelect {
    private static Logger baseSelect = Logger.getLogger("baseSelect");

    // uses gdelt data
    public HbaseBaseSelect(String[] args) throws ParseException {
        super(args, new HBaseDataStoreFactory().getParametersInfo(), new GDELTData());
        baseSelect.info("HbaseBaseSelect Init...");
    }

    public static void main(String[] args) {
        try {
            new HbaseBaseSelect(args).run();
        } catch (ParseException e) {
//            logger.error(e.printStackTrace());
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
