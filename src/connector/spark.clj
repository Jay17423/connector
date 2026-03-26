(ns connector.spark
  "Creates Spark session using Flambo (Spark 3)"
  (:require
   [mount.core :refer [defstate]]
   [flambo.session :as fs]
   [omniconf.core :as cfg]
   [taoensso.timbre :as log]))
(defn create-session
  []
  (log/info
   {:msg "creating spark session"
    :metric {:master (cfg/get :spark :app :master-url)
             :app-name (cfg/get :spark :app :name)}})

  (-> (fs/session-builder)
      (fs/master (cfg/get :spark :app :master-url))
      (fs/app-name (cfg/get :spark :app :name))
      (fs/config "spark.jars"
                 (str (System/getProperty "user.dir")
                      "/target/connector-0.1.0-SNAPSHOT-standalone.jar"))
      (fs/get-or-create)))

(defstate session
  :start
  (do
    (log/info "starting spark session")
    (try
      (create-session)
      (catch Exception e
        (throw e))))
  :stop
  (do
    (log/info "stopping spark session")
    (.stop session)))