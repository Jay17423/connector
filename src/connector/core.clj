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
            [connector.logger]
            [connector.config :as app-config]
            [connector.spark :refer [session]]
            [connector.dataset :as ds]
            [connector.specs :as spec]
            [omniconf.core :as cfg]
            [clojure.string :as str]
            [connector.utils :as util]))

(defn normalize-body [body]
  (let [{:keys [source auth options type format link-type]
         :or {link-type "public"}} body
        type-kw (keyword type)
        normalized {:type     type-kw
                    :format   format
                    :link-type (keyword link-type)
                    :options  options}]
    (case type-kw

      :local
      (assoc normalized :path (:path source))

      :gdrive
      (assoc normalized
             :cred {:link (:url source)
                    :token (:token auth)
                    :revision-id (:revision-id source)})

      :dropbox
      (assoc normalized
             :cred {:link (if (= :private (:link-type normalized))
                            (:path source)
                            (:url source))
                    :token (:token auth)
                    :revision-id (:revision-id source)})

      :s3
      (assoc normalized
             :cred {:bucket (:bucket source)
                    :key (:key source)
                    :region (:region source)
                    :access-key (:access-key auth)
                    :secret-key (:secret-key auth)
                    :version-id (:version-id source)})

      :gcs
      (assoc normalized
             :cred {:bucket (:bucket source)
                    :key (:key source)
                    :generation (:generation source)
                    :project-id (:project-id auth)
                    :client-email (:client-email auth)
                    :private-key (some-> (:private-key auth)
                                          (str/replace "\\n" "\n"))})
      normalized)))

(defn load-data
  "Handles dataset load request."
  [req]
  (let [start-time (System/currentTimeMillis)
        body (:body req)]
    (try
      (spec/validate-body! body)

      (log/info {:msg "dataset load request"
                 :metric {:type (:type body)}})

      (let [config (normalize-body body)
            dataset (ds/read-dataset session config)
            duration (- (System/currentTimeMillis) start-time)
            preview (util/dataset->preview dataset)]

        (log/info {:msg "dataset loaded successfully"
                   :metric {:type (:type body)
                            :duration-ms duration}})

        (-> (response {:status "success"
                       :source (:type body)
                       :duration-ms duration
                       :data preview
                       ;; :count (fsql/count df)
                       })
            (status 200)))

      (catch AssertionError err
        (let [duration (- (System/currentTimeMillis) start-time)]
          (log/error err {:msg "invalid request body"
                          :metric {:duration duration}})

          (-> (response {:status "error"
                         :error "invalid request body"
                         :duration duration})
              (status 400))))

      (catch Exception err
        (let [duration (- (System/currentTimeMillis) start-time)]
          (log/error err {:msg "dataset load failed"
                          :metric {:duration duration}})

          (-> (response {:status "error"
                         :error (.getMessage err)
                         :duration duration})
              (status 500)))))))

(defroutes app-routes
  "Defines API routes."
  (POST "/data-load" [] load-data)
  (route/not-found (-> (response {:error "NOT_FOUND"}) (status 404))))

(def app
  "Ring application with JSON middleware."
  (-> app-routes
      (wrap-json-body {:keywords? true})
      wrap-json-response))

(defn -main
  "Starts application and HTTP server."
  [] 
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