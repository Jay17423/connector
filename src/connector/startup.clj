(ns connector.startup
  "Startup dataset loader"
  (:require
   [connector.spark :refer [session]]
   [connector.dataset :as ds]
   [omniconf.core :as cfg]
   [flambo.sql :as fsql]
   [taoensso.timbre :as log]))


(defn load-initial-datasets
  []
  (let [datasets (cfg/get :spark :datasets)]
    (log/info
     {:msg "loading startup datasets"
      :metric {:datasets (keys datasets)}})

    (reduce-kv
     (fn [result k conf]

       (log/info
        {:msg "loading dataset"
         :metric {:dataset k}})

       (let [df (ds/read-dataset session conf)]
         (fsql/print-schema df)
         (fsql/show df 5 false)
         (assoc result k df)))
     {}
     datasets)))