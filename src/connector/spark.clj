(ns connector.spark
  "Creates Spark session using Flambo (Spark 3)"
  (:require
   [mount.core :refer [defstate]]
   [flambo.session :as fs]
   [omniconf.core :as cfg]
   [taoensso.timbre :as log]))
   
(defn create-session
  []

  (log/info
   {:msg "creating spark session"
    :metric {:master (cfg/get :spark :app :master-url)
             :app-name (cfg/get :spark :app :name)}})

  (let [home (System/getProperty "user.home")

        hadoop-aws-jar
        (str home
             "/.m2/repository/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar")

        aws-sdk-jar
        (str home
             "/.m2/repository/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar")]

    (-> (fs/session-builder)
        (fs/master (cfg/get :spark :app :master-url))
        (fs/app-name (cfg/get :spark :app :name))
        ;; send jars to executors
        (fs/config
         "spark.jars"
         (str
          (System/getProperty "user.dir")
          "/target/connector-0.1.0-SNAPSHOT-standalone.jar,"
          hadoop-aws-jar ","
          aws-sdk-jar))
        ;; required s3 configs
        (fs/config
         "spark.hadoop.fs.s3a.impl"
         "org.apache.hadoop.fs.s3a.S3AFileSystem")

        (fs/config
         "spark.hadoop.fs.s3a.path.style.access"
         "true")

        (fs/get-or-create))))

(defstate session
  :start
  (do (log/info "starting spark session")
      (try (create-session)
           (catch Exception e
             (throw e))))
  :stop
  (do (log/info "stopping spark session")
      (.stop session)))