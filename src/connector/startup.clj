(ns connector.startup
  "Startup dataset loader — loads all configured datasets in parallel."
  (:require
   [connector.spark  :refer [session]]
   [connector.dataset :as ds]
   [omniconf.core    :as cfg]
   [flambo.sql       :as fsql]
   [taoensso.timbre  :as log]))

(defn load-initial-datasets
  []
  (let [datasets (cfg/get :spark :datasets)]
    (log/info {:msg    "loading startup datasets"
               :metric {:count   (count datasets)
                        :sources (keys datasets)}})
    (->> datasets
         (pmap (fn [[k conf]]
                 (log/info {:msg "loading dataset" :metric {:dataset k}})
                 (let [df (ds/read-dataset session conf)]
                   (fsql/print-schema df)
                   (fsql/show df 5 false)
                   [k df])))
         (into {}))))