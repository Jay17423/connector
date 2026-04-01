(ns connector.dataset
  "Loads datasets into Spark using production-safe CSV options."
  (:require [flambo.sql :as fsql]
            [connector.http-fetcher :as fetcher]
            [taoensso.timbre :as log]))

(defn resolve-source
  [config]
  (let [{:keys [type path cred]} config]
    (cond
      (= type :local)
      [path false]

      (contains? #{:gdrive :dropbox :s3 :gcs} type)
      [(fetcher/fetch-file! type cred) true]

      :else
      (throw (ex-info
              "Unsupported dataset source"
              {:type type})))))

(defn read-dataset
  [spark {:keys [options] :as config}]

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