package com.sparkProject


import org.apache.spark.sql.SparkSession

object Job {

    def main(args: Array[String]): Unit = {

        // SparkSession configuration
        val spark = SparkSession
            .builder
            .appName("spark session TP_parisTech")
            .getOrCreate()

        /** ******************************************************************************
          *
          * TP 1
          *
          * - Set environment, IntelliJ, submit jobs to Spark
          * - Load local unstructured data
          * - Word count , Map Reduce
          * *******************************************************************************/


        // ----------------- word count ------------------------

        /* val df_wordCount = sc.textFile("/opt/spark-2.0.0-bin-hadoop2.7/README.md")
          .flatMap{case (line: String) => line.split(" ")}
          .map{case (word: String) => (word, 1)}
          .reduceByKey{case (i: Int, j: Int) => i + j}
          .toDF("word", "count")

        df_wordCount.orderBy($"count".desc).show()
        */

        /** ******************************************************************************
          *
          * TP 2 : début du projet
          *
          * *******************************************************************************/

        var df = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("comment", "#")
            .csv("cumulative.csv")


//        println("# of columns:" + df.columns.length)
//        println("# of lines:" + df.count())

//        val columnsToShow = df.columns.slice(0,10)
//        df.select(columnsToShow.head, columnsToShow.tail:_*).show(20)

        //        df.printSchema()
        //        df.groupBy("koi_disposition").count().show()

        // 4-a
        df = df.filter(df("koi_disposition") =!= "CANDIDATE")

        // 4-b
        //        df.groupBy("koi_eccen_err1").count().show()


        // 4-c
        df = df.drop("koi_eccen_err1")

        // 4-d
        df = df.drop("index", "kepid", "koi_fpflag_nt", "koi_fpflag_ss", "koi_fpflag_co",
            "koi_fpflag_ec", "koi_sparprov", "koi_trans_mod", "koi_datalink_dvr",
            "koi_datalink_dvs", "koi_tce_delivname", "koi_parm_prov", "koi_limbdark_mod",
            "koi_fittype", "koi_disp_prov", "koi_comment", "kepoi_name", "kepler_name",
            "koi_vet_date", "koi_pdisposition")

        //        df.printSchema()

        // 4-e
        val cols = df.columns
        var droppedCols = List[String]()
        for (col <- cols) {
            if (df.groupBy(col).count().count() <= 1) {
                droppedCols ++= List(col)
            }
        }

        df = df.drop(droppedCols:_*)
        println("Dropped columns:")
        for (col <- droppedCols) {
            println("\t" + col)
        }

        df.describe(df.columns.slice(0,10): _*).show()

        df = df.na.fill(0.0)

        // 6

//        // First select ID and koi_dispo, then drop koi_dispo in original dataframe
//        val df2 = df.select("rowid", "koi_disposition")
//        df = df.drop("koi_disposition")
//
//        // Now insert koi_dispo back in original dataframe
//        df = df.join(df2, usingColumn = "rowId")



        // 7

        df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv("cleanedDataframe.csv")

    }

}
