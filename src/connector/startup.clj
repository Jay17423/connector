(ns connector.startup
  "Startup dataset loader — loads configured dataset."
  (:require
   [connector.spark   :refer [session]]
   [connector.dataset :as ds]
   [omniconf.core     :as cfg]
   [flambo.sql        :as fsql]
   [taoensso.timbre   :as log]))

(defn load-initial-datasets
  []
  (let [conf (cfg/get :spark :datasets :gdrive-file)]

    (log/info
     {:msg "loading dataset"
      :metric {:source :gdrive-file}})

    (let [df (ds/read-dataset session conf)]
      (fsql/print-schema df)
      (fsql/show df 5 false)
      {:gdrive-file df})))