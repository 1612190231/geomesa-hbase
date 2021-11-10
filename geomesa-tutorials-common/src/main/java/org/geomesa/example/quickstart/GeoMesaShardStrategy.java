package org.geomesa.example.quickstart;

import org.locationtech.geomesa.index.api.ShardStrategy;
import org.locationtech.geomesa.index.api.WritableFeature;
import scala.collection.Seq;

/**
 * @author luchengkai
 * @description 分区实现类
 * @date 2021/11/10 20:43
 */
public class GeoMesaShardStrategy implements ShardStrategy {
    @Override
    public byte[] apply(WritableFeature feature) {
        return new byte[0];
    }

    @Override
    public Seq<byte[]> shards() {
        return null;
    }

    @Override
    public int length() {
        return 0;
    }
}
