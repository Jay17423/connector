(ns connector.core
  "Provides POST API to load dataset dynamically and return preview rows."
  (:gen-class)
  (:require [mount.core :as mount]
            [compojure.core :refer [defroutes POST]]
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

(defn normalize-body
  "Creates the config from the body. "
  [body]
  (let [{:keys [source auth options type format link-type]
         :or {link-type "public"}} body
        type-kw    (keyword type)
        normalized {:type type-kw
                    :format format
                    :link-type (keyword link-type)
                    :options options}]

    (case type-kw
      :local
      (assoc normalized :path (:path source))

      :gdrive
      (assoc normalized
             :cred {:link (:url source)
                    :token (:token auth)
                    :refresh-token (:refresh-token auth)
                    :client-id (:client-id auth)
                    :client-secret (:client-secret auth)
                    :revision-id (:revision-id source)})

      :dropbox
      (assoc normalized
             :cred {:link (if (= :private (keyword link-type))
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
      (throw (ex-info "Unsupported source type"
                      {:type type-kw})))))

(defn load-data
  "Handles dataset load request."
  [req]
  (let [start-time (System/currentTimeMillis)
        body (:body req)]
    (try
      (spec/validate-body! body)
      (log/info {:msg "Dataset load request"
                 :metric {:type (:type body)}})

      (let [config (normalize-body body)
            dataset (ds/read-dataset session config)
            duration (- (System/currentTimeMillis) start-time)
            preview (util/dataset->preview dataset)]

        (log/info {:msg "Dataset loaded successfully"
                   :metric {:type (:type body)
                            :duration-ms duration}})

        (-> (response {:status "success"
                       :source (:type body)
                       :duration-ms duration
                       :data preview})
            (status 200)))

      (catch AssertionError err
        (let [duration (- (System/currentTimeMillis) start-time)]
          (log/warn {:msg "Invalid request body"
                     :error (.getMessage err)
                     :metric {:duration-ms duration}})
          (-> (response {:status "error"
                         :msg "Invalid request body"
                         :error (.getMessage err)
                         :duration-ms duration})
              (status 400))))
      
      (catch Exception err
        (let [duration (- (System/currentTimeMillis) start-time)]
          (log/error {:msg "Dataset load failed"
                      :error (.getMessage err)
                      :type (-> err .getClass .getName)
                      :metric {:duration-ms duration}}) 
          (-> (response {:status "error"
                         :msg "Internal server error"
                         :duration-ms duration})
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
    (log/info {:msg "Starting connector service"})
    (app-config/load-config! "config/config.edn")
    (mount/start)
    (log/info {:msg "Starting http server"
               :metric {:port (cfg/get :server :port)}})
    (jetty/run-jetty app {:port (cfg/get :server :port)})
    (catch Exception err
      (log/error {:msg "Startup failed"
                  :error (.getMessage err)})
      (System/exit 1))))