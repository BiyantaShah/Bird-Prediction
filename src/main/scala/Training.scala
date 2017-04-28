import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


// Training class is used to train the model
object Training {

  override def finalize(): Unit = super.finalize()


  def main(args: Array[String]) {

    try {

      val conf = new SparkConf(true).setAppName("Training")
        .setMaster("local")
//          .setMaster("yarn")

      // Create a scala spark context
      val sc = new SparkContext(conf)


      // The documentation states that if the species is reported as present but without a count
      // then it is replaced by "x". Since the species is present, when we are encountering a "x" we
      // replace it with 1.0 while training the model.
      def change(s: String) : Double = {
        if (s.equalsIgnoreCase("x") || s.toDouble > 0.0) return 1.0
        return 0.0

      }

      // loading the labeled data and removing the header row
      // extracting the features for training the model.
      val labeledData = sc.textFile(args(0))
        .mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}
        .map(cols => {val lines  = cols.split(",")
                          List (lines(26),  lines(2),    lines(3),    lines(4),    lines(5),    lines(7),   lines(12),
                                            lines(13),   lines(14),   lines(16),
                                            lines(955),  lines(956),  lines(957),  lines(958),  lines(960), lines(962),
                                            lines(963),  lines(966),  lines(967),
                                            lines(1090), lines(1091), lines(1092), lines(1093), lines(1094),lines(1095),
                                            lines(1096), lines(1097), lines(1098), lines(1099), lines(1100), lines(1101))
                     })


      // When the value for a feature is missing or unknown it is represented as a "?". On encountering
      // a "?" we ignore the feature for that row.
      // We are returning a tuple which consists of the index of the array of the features
      // to be included for that row, and the value of those features
      def check(line: List[String]) : (Array[Int],Array[Double]) = {
        var res = ArrayBuffer[Double]()
        var indexList = ArrayBuffer[Int]()
        for( i <- 1 to line.size-1)
        {
          if(!line(i).equals("?") && !line(i).equalsIgnoreCase("x")) {
            res += line(i).toDouble
            indexList += i
          }
        }
        return (indexList.toArray,res.toArray)
      }


      // Creating a labeled point which contains the red winged bird as the label and
      // the rest of the features as features to predict on.
      val transformed = labeledData.filter(row => ! row.head.equals("?"))
        .map (line => (LabeledPoint(change(line.head),Vectors.sparse(line.size,
                                                                     check(line)._1,
                                                                     check(line)._2))))
//                                                                   .repartition(250)
// the above repartitioning is manual and can be used if more partitions are needed


      // The following code for training the model has been referenced from
      // https://spark.apache.org/docs/latest/mllib-ensembles.html

      // Splitting the transformed LabeledPoint RDD into 70% and 30% where
      // 70% is for training and 30% for testing
      val splits = transformed.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))



      // parameters for training random forest model
      val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]() // empty Map since no categorical features
      val numTrees = 15
      val featureSubsetStrategy = "auto" // Let the algorithm choose.
      // (for classification random forest chooses square root of the number of features)
      val impurity = "gini"
      val maxDepth = 18
      val maxBins = 32


      // training the model
      val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

      // Evaluate model on test instances and compute test error
      val labelAndPreds = testData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }

      val testError = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
      println("Accuracy% = " + ((1-testError)*100))

      // Saving the trained model
      model.save(sc,args(1))

    }
    catch {
      case e : Exception => println("Exception : " + e);
    }

  }
}
