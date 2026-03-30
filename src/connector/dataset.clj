(ns connector.dataset
  "Loads CSV datasets into Spark from local or Google Drive."
  (:require [flambo.sql       :as fsql]
            [connector.gdrive :as gdrive]
            [taoensso.timbre  :as log]))

(defn resolve-source
  "Returns file path; downloads if source is Google Drive."
  [config]
  (let [{:keys [type path cred options]} config]
    (cond
      (= type :local)
      [path false]

      (= type :gdrive)
      [(gdrive/fetch-and-clean! cred options) true]

      :else
      (throw
       (ex-info
        "Unsupported dataset source type"
        {:type      type
         :supported [:local :gdrive]})))))

(defn read-dataset
  "Reads CSV into Spark DataFrame using Flambo."
  [spark {:keys [options] :as config}]
  (let [[path temp?] (resolve-source config)]
    (log/info
     {:msg "reading csv dataset into Spark"
      :metric
      {:type  (:type config)
       :path  path
       :temp? temp?}})

    (fsql/read-csv
     spark
     path
     :header    (:header options true)
     :delimiter (:delimiter options ",")
     :mode "PERMISSIVE")))