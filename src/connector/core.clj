(ns connector.core
  "Bootstraps config, Spark lifecycle and HTTP server."
  (:gen-class)
  (:require
   [mount.core :as mount]
   [compojure.core :refer [defroutes GET]]
   [compojure.route :as route]
   [ring.util.response :refer [response]]
   [ring.adapter.jetty :as jetty]
   [ring.middleware.json :refer [wrap-json-response]]
   [taoensso.timbre :as log]
   [connector.config :as app-config]
   [connector.spark]
   [connector.logger]
   [omniconf.core :as cfg]
   [connector.startup :as startup]))

(defroutes app-routes
  (GET "/health" [] (response {:status "ok"}))
  (route/not-found (response {:error "NOT_FOUND"})))

(def app
  (-> app-routes
      wrap-json-response))

(defn -main
  []
  (try
    (log/info {:msg "starting connector service"})
    (app-config/load-config! "config/config.edn")
    (mount/start)
    (startup/load-initial-datasets)
    (log/info
     {:msg "starting http server"
      :metric {:port (cfg/get :server :port)}})

    (jetty/run-jetty app {:port (cfg/get :server :port)})

    (catch Exception err
      (log/error err "startup failed")
      (System/exit 1))))