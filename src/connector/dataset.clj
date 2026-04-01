(ns connector.dataset
  "Loads datasets into Spark."
  (:require [flambo.sql :as fsql]
            [connector.http-fetcher :as fetcher]
            [taoensso.timbre :as log]))

(defn resolve-source
  "Returns file path; triggers download and cleaning for cloud sources."
  [config]
  (let [{:keys [type path cred options]} config]
    (cond
      (= type :local) 
      [path false]

      (contains? #{:gdrive :dropbox :s3 :gcs} type)
      [(fetcher/fetch-and-clean! type cred options) true]

      :else (throw (ex-info "Unsupported dataset source" {:type type})))))

(defn read-dataset
  "Reads the (cleaned) CSV into a Spark DataFrame."
  [spark {:keys [options] :as config}]
  (let [[path temp?] (resolve-source config)]
    (log/info {:msg "reading dataset into spark"
               :metric {:type (:type config) :path path :temp? temp?}})
    (fsql/read-csv spark path
                   :header (:header options true)
                   :delimiter (:delimiter options ",")
                   :mode "PERMISSIVE")))