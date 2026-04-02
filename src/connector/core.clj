(ns connector.core
  "Provides POST API to load dataset dynamically and return preview rows."
  (:gen-class)
  (:require [mount.core :as mount]
            [compojure.core :refer [defroutes GET POST]]
            [compojure.route :as route]
            [ring.util.response :refer [response status]]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.json :refer [wrap-json-body wrap-json-response]]
            [taoensso.timbre :as log]
            [flambo.sql :as fsql]
            [connector.config :as app-config]
            [connector.spark :refer [session]]
            [connector.dataset :as ds]
            [connector.specs :as spec]
            [clojure.spec.alpha :as s]
            [omniconf.core :as cfg]
            [connector.utils :as util]))

(defn normalize-body
  "Normalizes POST body and prepares credentials for all cloud source types."
  [body]
  (let [body-with-defaults (merge {:link-type "public"} body)
        normalized (-> body-with-defaults
                       (update :type keyword)
                       (update :link-type keyword))]
    (cond
      (contains? #{:gdrive :dropbox :gcs} (:type normalized))
      (assoc normalized :cred {:link (:link normalized)
                               :token (:token normalized)
                               :revision-id (:revision-id normalized)})

      (= :s3 (:type normalized))
      (assoc normalized :cred {:bucket (:bucket normalized)
                               :key (:key normalized)
                               :region (:region normalized)
                               :access-key (:access-key normalized)
                               :secret-key (:secret-key normalized)
                               :version-id (:version-id normalized)})
      :else
      normalized)))

(defn load-data 
  [req] 
  (let [body (:body req)
        start-time (System/currentTimeMillis)]
    (try
      (log/info {:msg "dataset load request"
                 :metric {:type (:type body)}})

      (let [config (normalize-body body) 
            df (ds/read-dataset session config)
            duration (- (System/currentTimeMillis) start-time)
            preview (util/df->json-preview df)]

        (log/info {:msg "dataset loaded successfully"
                   :metric {:type (:type body)
                            :duration_ms duration}})

        (-> (response {:status "success"
                       :source (:type config)
                       :duration_ms duration
                       :count (fsql/count df)
                       :rows preview})
            (status 200)))
      
      (catch Exception e
        (let [duration (- (System/currentTimeMillis) start-time)]
          (log/error e {:msg "dataset load failed"
                        :metric {:duration_ms duration}})

          (-> (response {:status "error"
                         :error (.getMessage e)
                         :duration_ms duration})
              (status 500)))))))

(defroutes app-routes
  (GET "/health" [] (response {:status "ok"}))
  (POST "/data-load" [] load-data)
  (route/not-found (-> (response {:error "NOT_FOUND"}) (status 404))))

(def app
  (-> app-routes
      (wrap-json-body {:keywords? true})
      wrap-json-response))

(defn -main []
  (try
    (log/info {:msg "starting connector service"})
    (app-config/load-config! "config/config.edn")
    (mount/start)
    (log/info {:msg "starting http server" 
               :metric {:port (cfg/get :server :port)}})
    (jetty/run-jetty app {:port (cfg/get :server :port)})
    (catch Exception err
      (log/error err "startup failed")
      (System/exit 1))))