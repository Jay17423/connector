(ns connector.dataset
  (:require
   [flambo.sql :as fsql]
   [taoensso.timbre :as log]))


(defn resolve-path
  [{:keys [type path cred]}]

  (case type

    :local path
    (throw
     (ex-info "Unsupported source"
              {:type type}))))


(defn read-dataset
  [spark {:keys [options] :as config}]

  (let [resolved-path
        (resolve-path config)]

    (log/info
     {:msg "reading csv dataset"
      :metric {:path resolved-path}})

    (fsql/read-csv
     spark
     resolved-path
     :header (:header options)
     :delimiter (:delimiter options))))