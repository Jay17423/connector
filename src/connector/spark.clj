(ns connector.spark
  "Creates Spark session using Flambo (Spark 3)"
  (:require
   [mount.core :refer [defstate]]
   [flambo.session :as fs]
   [flambo.sql :as sql]
   [omniconf.core :as cfg]
   [taoensso.timbre :as log]))

(defn create-session
  "Creates configured SparkSession with required classpath and cloud filesystem
   support."
  []
  (try
    (-> (fs/session-builder)
        (fs/master (cfg/get :spark :app :master-url))
        (fs/app-name (cfg/get :spark :app :name))
        ;; fat jar classpath
        (fs/config
         "spark.executor.extraClassPath"
         (str (System/getProperty "user.dir")
              "/target/connector-0.1.0-SNAPSHOT-standalone.jar"))
        (fs/config
         "spark.driver.extraClassPath"
         (str (System/getProperty "user.dir")
              "/target/connector-0.1.0-SNAPSHOT-standalone.jar"))
        (fs/config
         "spark.jars"
         (str (System/getProperty "user.dir")
              "/target/connector-0.1.0-SNAPSHOT-standalone.jar"))
        ;; Amazon S3
        (fs/config
         "spark.hadoop.fs.s3a.impl"
         "org.apache.hadoop.fs.s3a.S3AFileSystem")
        (fs/config
         "spark.hadoop.fs.s3a.path.style.access"
         "true")
        ;; Google Cloud Storage
        (fs/config
         "spark.hadoop.fs.gs.impl"
         "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        (fs/config
         "spark.hadoop.fs.AbstractFileSystem.gs.impl"
         "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

        (fs/get-or-create))

    (catch Exception err
      (throw (ex-info "Unable to create Spark session"
                      {:type :spark/session-create-failed
                       :master (cfg/get :spark :app :master-url)
                       :app-name (cfg/get :spark :app :name)
                       :error (.getMessage err)}
                      err)))))

(defn warmup-spark
  "Executes small job to initialize Spark executors and reduce first-query
   latency."
  [spark]
  (try
    (log/info {:msg "Spark warmup starting"})
    (sql/count (sql/create-dataset spark [1 2 3 4]))
    (log/info {:msg "Spark warmup completed"})
    spark
    (catch Exception err
      (throw (ex-info "Spark warmup failed"
                      {:type :spark/warmup-failed
                       :error (.getMessage err)}
                      err)))))

(defstate session
  "Mount-managed lifecycle state for starting and stopping Spark session
   safely."
  :start
  (let [spark (create-session)]
    (warmup-spark spark)
    spark)
  :stop
  (try
    (when session
      (.stop session))
    (catch Exception e
      (log/error
       {:msg "Error while stopping Spark session"
        :error (.getMessage e)}))))