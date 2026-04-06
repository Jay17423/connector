(ns connector.logger
  "Configures Timbre with rolling log files."
  (:require
   [taoensso.timbre :as log]
   [taoensso.timbre.appenders.community.rolling :as rolling]))

(.mkdirs (java.io.File. "logs"))

(log/merge-config!
 {:level :info
  :appenders
  {:rolling (rolling/rolling-appender
             {:path    (str "logs" "/app.log")
              :pattern :daily})}})