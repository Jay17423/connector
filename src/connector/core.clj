(ns connector.core
  "Provides POST API to load dataset dynamically and return preview rows."
  (:gen-class)
  (:require [mount.core :as mount]
            [compojure.core :refer [defroutes POST]]
            [compojure.route :as route]
            [ring.util.response :refer [response status]]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.json :refer [wrap-json-body wrap-json-response]]
            [connector.logger :as logger]
            [taoensso.timbre :as log]
            [connector.spark :refer [session]]
            [connector.dataset :as ds]
            [connector.specs :as spec]
            [omniconf.core :as cfg]
            [clojure.string :as str]
            [connector.utils :as util]
            [connector.middleware :as mw]))

(defn normalize-body
  "Creates the common config for all sources from the body. "
  [body]
  (let [{:keys [source auth options type format link-type]
         :or {link-type "public"}} body
        type-kw (keyword type)
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
  ((spec/validate-body! (:body req))
   (log/info {:msg "Dataset load request"
              :metric {:type (:type (:body req))}})
   (let [config (normalize-body (:body req))
         dataset (ds/read-dataset session config)
         duration (- (System/currentTimeMillis) (:start-time req))
         preview (util/dataset->preview dataset)]

     (log/info {:msg "Dataset loaded successfully"
                :metric {:type (:type (:body req))
                         :duration-ms duration}})
     (status
      (response
       {:status "success",
        :source (:type (:body req)),
        :duration-ms duration,
        :data preview})
      200))))

(defroutes app-routes
  "Defines API routes."
  (POST "/data-load" [] load-data)
  (route/not-found (status (response {:error "NOT_FOUND"}) 404)))

(def app
  "Ring application with JSON middleware."
  (-> app-routes
      (mw/wrap-error-handler)
      (mw/wrap-request-timer)
      (wrap-json-body {:keywords? true})
      wrap-json-response))

(defn -main
  "Starts application and HTTP server."
  []
  (try
    (cfg/populate-from-file "config/config.edn")
    (cfg/verify)
    (logger/configure-logger!)
    (log/info {:msg "Starting connector service"})
    (mount/start)
    (log/info {:msg "Starting http server"
               :metric {:port (cfg/get :server :port)}})
    (jetty/run-jetty app {:port (cfg/get :server :port)})
    (catch Exception err
      (log/error {:msg "Startup failed"
                  :error (.getMessage err)})
      (System/exit 1))))