(ns connector.config
  "Handles loading and accessing application configuration."
  (:require 
   [omniconf.core :as cfg]))

(defn load-config!
  "Load and verify application configuration"
  [path]
  (try (cfg/populate-from-file path)
       (cfg/verify)
       (catch Exception err (throw err))))