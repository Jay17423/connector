(ns connector.logger
  "Configures Timbre logging with daily rotating log files stored in the logs
   directory."
  (:require
   [taoensso.timbre :as log]
   [taoensso.timbre.appenders.core :as appenders]
   [clj-time.core :as time]
   [clj-time.format :as format]))

(def date-formatter
  (format/formatter "yyyy-MM-dd"))

(defn today-log-file
  "Returns log file path using current date (yyyy-MM-dd) inside logs folder."
  []
  (str "logs/app-"
       (format/unparse
        date-formatter
        (time/now))
       ".log"))

(log/merge-config!
 {:level :info
  :appenders
  {:spit
   (appenders/spit-appender
    {:fname (today-log-file)
     :append? true})}})