(ns connector.utils
  "Provides shared helper utilities for common reusable functions across
   connector modules."
  (:require [flambo.sql :as fsql]))

(defn dataset->preview 
  "Converts first 10 rows of Spark Dataset to a list of maps."
  [dataset]
  (let [columns (seq (.columns dataset))]
    (->> (.limit dataset 10)
         fsql/collect
         (map (fn [row]
                (into {} (map (fn [col]
                                [col (.getAs row col)])
                              columns)))))))