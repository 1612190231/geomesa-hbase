package org.geomesa.example.index;

import org.locationtech.geomesa.index.api.IndexAdapter;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory;
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata;
import org.locationtech.geomesa.index.stats.GeoMesaStats;
import org.locationtech.geomesa.index.utils.Releasable;
import scala.Option;

/**
 * @author luchengkai
 * @description 分区数据库创建类
 * @date 2021/11/10 21:45
 */
public class ShardGeoMesaDataStore extends GeoMesaDataStore {
    public ShardGeoMesaDataStore(GeoMesaDataStoreFactory.GeoMesaDataStoreConfig config) {
        super(config);
    }

    @Override
    public IndexAdapter adapter() {
        return null;
    }

    @Override
    public GeoMesaStats stats() {
        return null;
    }

    @Override
    public Releasable acquireDistributedLock(String key) {
        return null;
    }

    @Override
    public Option<Releasable> acquireDistributedLock(String key, long timeOut) {
        return null;
    }

    @Override
    public GeoMesaMetadata<String> metadata() {
        return null;
    }
}
