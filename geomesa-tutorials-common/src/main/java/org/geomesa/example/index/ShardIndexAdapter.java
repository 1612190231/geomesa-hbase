package org.geomesa.example.index;

import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex;
import org.locationtech.geomesa.index.api.IndexAdapter;
import org.locationtech.geomesa.index.api.QueryPlan;
import org.locationtech.geomesa.index.conf.ColumnGroups;
import org.opengis.feature.simple.SimpleFeatureType;
import scala.Function0;
import scala.Option;
import scala.collection.Seq;

/**
 * @author luchengkai
 * @description 索引构建类
 * @date 2021/11/10 21:58
 */
public class ShardIndexAdapter(ShardGeoMesaDataStore ds) implements IndexAdapter {
    @Override
    public ColumnGroups groups() {
        return null;
    }

    @Override
    public Option<Object> tableNameLimit() {
        return null;
    }

    @Override
    public void createTable(GeoMesaFeatureIndex index, Option partition, Function0 splits) {

    }

    @Override
    public void renameTable(String from, String to) {

    }

    @Override
    public QueryPlan createQueryPlan(QueryStrategy strategy) {
        return null;
    }

    @Override
    public IndexWriter createWriter(SimpleFeatureType sft, Seq indices, Option partition) {
        return null;
    }

    @Override
    public void clearTables(Seq tables, Option prefix) {

    }

    @Override
    public void deleteTables(Seq tables) {

    }
}
