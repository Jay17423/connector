(ns connector.logger
  "Configures Timbre with rolling log files based on environment."
  (:require
   [taoensso.timbre :as log]
   [taoensso.timbre.appenders.community.rolling :as rolling]
   [omniconf.core :as cfg]))

(defn configure-logger!
  "Configure log level and rolling file appender using config values"
  []
  (.mkdirs (java.io.File. (cfg/get :logging :dir)))
  (log/merge-config!
   {:level (cfg/get :logging :level)
    :appenders {:rolling (rolling/rolling-appender
                          {:path    (str (cfg/get :logging :dir) "/" (cfg/get :logging :file))
                           :pattern :daily})}}))