import org.apache.spark.{SparkConf, SparkContext}


object Enron extends App {

        val inputPath = "data/Enron_email.txt"

        val conf = new SparkConf().
                        setAppName("Spark Enron").
                        setMaster("local").
                        set("spark.ui.port", "45678")

        val sc = new SparkContext(conf)

        val readFileRdd = sc.textFile(inputPath)

        //      val inputPath = args(0)
        //      val outputPath = args(1)
        //      val readFileRdd = sc.textFile(inputPath.trim)

        /* To find the average length in words */

        val org_msg = "-----Original Message-----"
        val email_body = readFileRdd.filter(r => r.indexOf(".pst") > 0).
          map(r => {
            val startindex = r.indexOf(".pst")
            val endindex = if (r.contains(org_msg)) r.indexOf(org_msg) else r.length
            r.substring(startindex, endindex).replace(".pst", "")
          }).filter(r => !r.isEmpty)

        val email_total = email_body.count
        val email_words_sum = email_body.map(r => r.split(" ").length).reduce((a, b) => a + b)
        val email_avg_words_per_email = email_words_sum.toDouble / email_total
        println("The average length in words of email is : " + email_avg_words_per_email)


        /* To find the recepients of top 100 emails */
        val email_cc = readFileRdd.filter(r => r.indexOf("Cc:") > 0 && r.indexOf("Mime-Version:") > 0 && r.indexOf("Cc:") < r.indexOf("Mime-Version:"))
          .map(r => {
            val startindex = r.indexOf("Cc:")
            val endindex = r.indexOf("Mime-Version:")
            r.substring(startindex, endindex)
          }.replace("Cc:", "").trim)

        val email_to = readFileRdd.filter(r => r.indexOf("To:") > 0 && r.indexOf("Subject:") > 0 && r.indexOf("To:") < r.indexOf("Subject:"))
          .map(r => {
            val startindex = r.indexOf("To:")
            val endindex = r.indexOf("Subject:")
            r.substring(startindex, endindex)
          }.replace("To:", ""))

        val email_cc_List = email_cc.flatMap(r => r.split(",")).map(r => (r.trim, 1)).groupByKey
        val email_cc_List_sum = email_cc_List.mapValues(r => r.sum / 2)

        val email_to_List = email_cc.flatMap(r => r.split(",")).map(r => (r.trim, 1)).groupByKey
        val email_to_List_sum = email_to_List.mapValues(r => r.sum)

        val email_join = email_to_List_sum.join(email_cc_List_sum)
        val email_top_100 = email_join.mapValues(r => r._1 + r._2).sortBy(r => -r._2)

        email_top_100.map(r => r._1 + "\t" + r._2).foreach(println)
        //email_top_100.map(r => r._1 +"\t"+ r._2).saveAsTextFile(outputPath)


}
