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
  (let [source (or (:source body) {})
        auth (or (:auth body) {})
        options (or (:options body) {})
        link-type (keyword (or (:link-type body) "public"))
        type-kw (keyword (:type body))

        src-url (or (:url source) (:link source) (:link body))
        src-path (or (:path source) (:path body))

        revision-id (or (:revision-id source) (:revision-id body))
        version-id (or (:version-id source) (:version-id body))
        generation (or (:generation source) (:generation body))

        bucket (or (:bucket source) (:bucket body))
        key (or (:key source) (:key body))
        region (or (:region source) (:region body))

        token (or (:token auth) (:token body))
        access-key (or (:access-key auth) (:access-key body))
        secret-key (or (:secret-key auth) (:secret-key body))
        project-id (or (:project-id auth) (:project-id body))
        client-email (or (:client-email auth) (:client-email body))
        private-key (or (:private-key auth) (:private-key body))

        normalized {:type type-kw
                    :format (:format body)
                    :link-type link-type
                    :options options}]

    (cond

      ;; local file
      (= :local type-kw)
      (assoc normalized :path src-path)

      ;; gdrive uses url + token
      (= :gdrive type-kw)
      (assoc normalized
             :cred {:link src-url
                    :token token
                    :revision-id revision-id})

      ;; dropbox uses url for public, path for private
      (= :dropbox type-kw)
      (assoc normalized
             :cred {:link (if (= :private link-type) src-path src-url)
                    :token token
                    :revision-id revision-id})

      ;; amazon s3
      (= :s3 type-kw)
      (assoc normalized
             :cred {:bucket bucket
                    :key key
                    :region region
                    :access-key access-key
                    :secret-key secret-key
                    :version-id version-id})

      ;; google cloud storage
      (= :gcs type-kw)
      (assoc normalized
             :cred {:bucket bucket
                    :key key
                    :generation generation
                    :project-id project-id
                    :client-email client-email
                    :private-key private-key})

      :else
      normalized)))

(defn load-data
  [req]
  (let [body (:body req)
        start-time (System/currentTimeMillis)]
    (try
      (spec/validate-body! body)

      (log/info {:msg "dataset load request"
                 :metric {:type (:type body)}})

      (let [config (normalize-body body)
            df (ds/read-dataset session config)
            duration (- (System/currentTimeMillis) start-time)
            preview (util/df->json df)]

        (log/info {:msg "dataset loaded successfully"
                   :metric {:type (:type body)
                            :duration duration}})

        (-> (response {:status "success"
                       :source (:type config)
                       :duration duration
                       ;;  :count (fsql/count df)
                       :rows preview})
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