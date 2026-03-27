(ns connector.dataset
  (:require
   [flambo.sql        :as fsql]
   [connector.gdrive  :as gdrive]
   [taoensso.timbre   :as log]))

(defmulti ^:private resolve-source
  (fn [config] (:type config)))

(defmethod resolve-source :local
  [{:keys [path]}]
  [path false])

(defmethod resolve-source :gdrive
  [{:keys [cred options]}]
  [(gdrive/fetch-and-clean! cred options) true])

(defmethod resolve-source :default
  [{:keys [type]}]
  (throw (ex-info "Unsupported dataset source type"
                  {:type      type
                   :supported [:local :gdrive]})))

(defn read-dataset
  [spark {:keys [options] :as config}]
  (let [[path temp?] (resolve-source config)]
    (log/info {:msg    "reading csv dataset into Spark"
               :metric {:type  (:type config)
                        :path  path
                        :temp? temp?}})
    (fsql/read-csv spark
                   path
                   :header    (:header options true)
                   :delimiter (:delimiter options ","))))