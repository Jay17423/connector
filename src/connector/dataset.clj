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

(defn configure-gcs!
  [spark {:keys [project-id client-email private-key]}]

  (-> spark
      (.sparkContext)
      (.hadoopConfiguration)
      (.set "fs.gs.impl"
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"))

  (-> spark
      (.sparkContext)
      (.hadoopConfiguration)
      (.set "fs.AbstractFileSystem.gs.impl"
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"))

  (when project-id

    (-> spark
        (.sparkContext)
        (.hadoopConfiguration)
        (.set "fs.gs.project.id" project-id)))

  (when client-email

    (-> spark
        (.sparkContext)
        (.hadoopConfiguration)
        (.set
         "google.cloud.auth.service.account.email"
         client-email)))

  (when private-key

    (-> spark
        (.sparkContext)
        (.hadoopConfiguration)
        (.set
         "google.cloud.auth.service.account.private.key"
         private-key)))

  (-> spark
      (.sparkContext)
      (.hadoopConfiguration)
      (.set
       "google.cloud.auth.service.account.enable"
       "true")))

(defn resolve-source
  [config]

  (let [{:keys [type path cred]} config]

    (cond

      ;; local file
      (= type :local)

      [path false]

      ;; amazon s3
      (= type :s3)

      [(str
        "s3a://"
        (:bucket cred)
        "/"
        (:key cred))
       false]

      ;; google cloud storage
      (= type :gcs)

      [(str
        "gs://"
        (:bucket cred)
        "/"
        (:key cred)
        (when-let [g (:generation cred)]

          (str "#" g)))
       false]

      ;; gdrive + dropbox download temp file
      (contains? #{:gdrive :dropbox} type)

      [(fetcher/fetch-file! type cred) true]

      :else

      (throw
       (ex-info
        "Unsupported dataset source"
        {:type type})))))

(defn read-dataset
  [spark {:keys [options cred] :as config}]

  ;; s3 config
  (when (= :s3 (:type config))

    (configure-s3! spark cred))

  ;; gcs config
  (when (= :gcs (:type config))

    (configure-gcs! spark cred))

  (let [opts (merge {:header true
                     :delimiter ","
                     :inferSchema true
                     :multiLine false
                     :encoding "UTF-8"}
                    options)

        [path temp?]

        (resolve-source config)]

    (log/info
     {:msg "reading dataset into spark"
      :metric {:type (:type config)
               :path path
               :temp? temp?}})

    (fsql/read-csv
     spark
     path
     :header (:header opts)
     :delimiter (:delimiter opts)
     :inferSchema (:inferSchema opts)
     :mode "PERMISSIVE"
     :nullValue ""
     :ignoreLeadingWhiteSpace true
     :ignoreTrailingWhiteSpace true
     :encoding (:encoding opts)
     :multiLine (:multiLine opts)
     :columnNameOfCorruptRecord "_corrupt_record"
     :maxColumns 20480)))