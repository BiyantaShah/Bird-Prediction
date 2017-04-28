import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.model.RandomForestModel

import scala.collection.mutable.ArrayBuffer

// Prediction class is used to predict the sightings of AGELAIUS_PHOENICEUS
object Prediction {

  def main(args: Array[String]) :Unit ={

    val conf = new SparkConf(true).setAppName("Prediction")
      .setMaster("local")
//          .setMaster("yarn")

    // Create a scala spark context
    val sc = new SparkContext(conf)

    // Loading the created model
    val model = RandomForestModel.load(sc,args(0))


    // When the value for a feature is missing or unknown it is represented as a "?". On encountering
    // a "?" we ignore that feature for that row.
    // We are returning a tuple which consists of the index of the array of the features
    // to be included for that row, and the value of those features
    def check(line: List[String]) : (Array[Int],Array[Double]) = {
      var res = ArrayBuffer[Double]()
      var indexList = ArrayBuffer[Int]()
      for( i <- 1 to line.size-1)
      {
        if(!line(i).equals("?")) {
          res += line(i).toDouble
          indexList += i
        }
      }
      return (indexList.toArray,res.toArray)
    }

    // loading the unlabeled data and removing the header row
    // extracting the exact same features included while training the model except
    // the red winged blackbird which is not added since all the values are "?" and we need to predict it
    // and have included the event_sampling_id due to the required output format
    val unlabeled = sc.textFile(args(1))
            .mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}
            .map(cols => {val lines  = cols.split(",")
              List(lines(0), lines(2),    lines(3),    lines(4),   lines(5),     lines(7),    lines(12),   lines(13),
                             lines(14),   lines(16),
                             lines(955),  lines(956),  lines(957),  lines(958),  lines(960),
                             lines(962),  lines(963),  lines(966),  lines(967),
                             lines(1090), lines(1091), lines(1092), lines(1093), lines(1094), lines(1095), lines(1096),
                             lines(1097), lines(1098), lines(1099), lines(1100), lines(1101))})

    // Predicting the output from the trained model
    val transformUnlabel = unlabeled.map(line => (line.head, model.predict(Vectors.sparse(line.size,
                                                                                          check(line)._1,
                                                                                          check(line)._2))))

    // Saving it to the output file in required format
    val formattedData = transformUnlabel.map(line => line._1 + "," + line._2)

    val headerRow = sc.parallelize(Array("SAMPLING_EVENT_ID,SAW_AGELAIUS_PHOENICEUS"))

    sc.parallelize(headerRow.union(formattedData).collect(),1).saveAsTextFile(args(2))


  }

}
