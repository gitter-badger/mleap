package org.apache.mleap.runtime

/**
  * Created by hwilkins on 11/2/15.
  */
trait Dataset extends Serializable {
   def map(f: (Row) => Row): Dataset
 }
