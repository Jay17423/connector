(ns connector.dataset
  "Loads datasets into Spark using production-safe CSV options."
  (:require [flambo.sql :as fsql]
            [connector.http-fetcher :as fetcher]
            [taoensso.timbre :as log]))

(defn configure-s3!
  [spark {:keys [access-key secret-key region version-id]}]
  (when access-key
    (-> spark
        (.sparkContext)
        (.hadoopConfiguration)
        (.set "fs.s3a.access.key" access-key)))
  
  (when secret-key
    (-> spark
        (.sparkContext)
        (.hadoopConfiguration)
        (.set "fs.s3a.secret.key" secret-key)))

  (when region
    (-> spark
        (.sparkContext)
        (.hadoopConfiguration)
        (.set "fs.s3a.endpoint" (str "s3." region ".amazonaws.com"))))

  (when version-id
    (-> spark
        (.sparkContext)
        (.hadoopConfiguration)
        (.set "fs.s3a.version.id" version-id)))

  (-> spark
      (.sparkContext)
      (.hadoopConfiguration)
      (.set "fs.s3a.impl" "org.apache.hadoop.fs.s3a.S3AFileSystem")))

(defn resolve-source
  [config]
  (let [{:keys [type path cred]} config]

    (cond
      (= type :local)
      [path false]

      (= type :s3)
      [(fetcher/fetch-file! type cred) false]

      (contains? #{:gdrive :dropbox :gcs} type)
      [(fetcher/fetch-file! type cred) true]

      :else
      (throw (ex-info
              "Unsupported dataset source"
              {:type type})))))

(defn read-dataset
  [spark {:keys [options cred] :as config}]

  (when (= :s3 (:type config))

    (configure-s3! spark cred))

  (let [[path temp?]

        (resolve-source config)]

    (log/info
     {:msg "reading dataset into spark"
      :metric {:type (:type config)
               :path path
               :temp? temp?}})

    (fsql/read-csv
     spark
     path
     :header true
     :delimiter (:delimiter options ",")
     :mode "PERMISSIVE"
     :nullValue ""
     :ignoreLeadingWhiteSpace true
     :ignoreTrailingWhiteSpace true
     :encoding "UTF-8"
     :multiLine false
     :columnNameOfCorruptRecord "_corrupt_record"
     :maxColumns 20480)))