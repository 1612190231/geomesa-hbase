package org.geomesa.example.quickstart

import org.locationtech.geomesa.index.api.{ShardStrategy, WritableFeature}
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ShardStart {
  def startShard(sft: SimpleFeatureType, sf: SimpleFeature): Unit ={
    val wrapper = WritableFeature.wrapper(sft, new ColumnGroups)
    val writable = wrapper.wrap(sf)
    val strategy = ShardStrategy(60)
    strategy.apply(writable)
  }
}
