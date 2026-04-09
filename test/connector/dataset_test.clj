(ns connector.dataset-test
  (:require [clojure.test :refer [deftest is]]
            [connector.dataset :as dataset]
            [connector.cloud-fetch :as fetcher]
            [flambo.sql :as fsql]
            [taoensso.timbre :as log]))

(deftest resolve-source-positive-and-edge
  (is (= "/tmp/a.csv"
         (dataset/resolve-source {:type :local :path "/tmp/a.csv"})))
  (is (= "s3a://bucket/key.csv"
         (dataset/resolve-source {:type :s3
                                  :cred {:bucket "bucket" :key "key.csv"}})))
  (is (= "gs://b/k#10"
         (dataset/resolve-source {:type :gcs
                                  :cred {:bucket "b"
                                         :key "k"
                                         :generation "10"}})))
  (with-redefs [fetcher/fetch-file! (fn [_ _] "/tmp/fetched.csv")]
    (is (= "/tmp/fetched.csv"
           (dataset/resolve-source {:type :dropbox :cred {:link "x"}})))))

(deftest resolve-source-negative
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Unsupported dataset source"
                        (dataset/resolve-source {:type :unknown}))))

(deftest read-dataset-calls-reader-with-defaults
  (let [calls (java.util.ArrayList.)]
    (with-redefs [connector.dataset/resolve-source (fn [_] "/tmp/a.csv")
                  connector.dataset/configure-s3! (fn [_ _] (.add calls :s3))
                  connector.dataset/configure-gcs! (fn [_ _] (.add calls :gcs))
                  log/info (fn [_])
                  fsql/read-csv
                  (fn [_ path & args]
                    (.add calls {:path path :args args})
                    :ok)]
      (is (= :ok
             (dataset/read-dataset :spark {:type :s3
                                           :cred {:bucket "b" :key "k"}
                                           :options {}})))
      (is (= :s3 (first calls)))
      (is (= "/tmp/a.csv" (:path (second calls))))
      (is (= true (get (apply hash-map (:args (second calls))) :header)))
      (is (= "," (get (apply hash-map (:args (second calls))) :delimiter))))))

(deftest read-dataset-negative
  (with-redefs [connector.dataset/resolve-source (fn [_] "/tmp/a.csv")
                fsql/read-csv
                (fn [& _] (throw (RuntimeException. "reader failed")))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Failed to read dataset"
                          (dataset/read-dataset :spark {:type :local
                                                        :path "/tmp/a.csv"
                                                        :options {}})))))