(ns connector.config
  "Handles loading and accessing application configuration."
  (:require
   [omniconf.core :as cfg]))

(defn load-config!
  "Loads and verifies application configuration from the given path."
  [path]
  (cfg/populate-from-file path)
  (cfg/verify))