(ns connector.logger
  "Configures Timbre with rolling log files based on environment."
  (:require
   [taoensso.timbre :as log]
   [taoensso.timbre.appenders.community.rolling :as rolling]
   [omniconf.core :as cfg]))

(defn env->log-level
  "Set log level and default log level is :info"
  [env]
  (case env
    :dev  :debug
    :prod :info
    :info))

(defn configure-logger!
  "Configure log level and rolling file appender using config values"
  []
  (let [env (cfg/get :env)
        level (env->log-level env)
        log-dir (cfg/get :logging :dir)
        log-file (cfg/get :logging :file)
        full-path (str log-dir "/" log-file)]
    ;; ensure directory exists
    (.mkdirs (java.io.File. log-dir))
    (log/merge-config!
     {:level level
      :appenders
      {:rolling
       (rolling/rolling-appender
        {:path full-path
         :pattern :daily})}})))