(ns connector.spark-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [connector.spark :as spark]
   [flambo.session :as fs]
   [flambo.sql :as sql]
   [omniconf.core :as cfg]))

(defn load-config-fixture [test-fn]
  ;; load actual config file
  (cfg/populate-from-file "config/config.edn")
  ;; ensure config is valid
  (cfg/verify)
  (test-fn))

(use-fixtures :once load-config-fixture)

(deftest create-session-success
  (with-redefs
   [fs/session-builder (fn [] :builder)
    fs/master (fn [builder _] builder)
    fs/app-name (fn [builder _] builder)
    fs/config (fn [builder _ _] builder)
    fs/get-or-create (fn [_] :mock-spark)]
    (is (= :mock-spark (spark/create-session)))))

(deftest create-session-failure
  (with-redefs
   [fs/session-builder
    (fn []
      (throw (Exception. "boom")))]
    (is
     (thrown-with-msg?
      clojure.lang.ExceptionInfo
      #"Unable to create Spark session"
      (spark/create-session)))))

(deftest warmup-spark-success

  (with-redefs
   [sql/create-dataset (fn [_ _] :dataset)
    sql/count (fn [_] 4)]
    (is (= :spark
           (spark/warmup-spark :spark)))))

(deftest warmup-spark-failure
  (with-redefs
   [sql/create-dataset (fn [_ _] :dataset)
    sql/count
    (fn [_]
      (throw (Exception. "warmup failed")))]
    (is
     (thrown-with-msg?
      clojure.lang.ExceptionInfo
      #"Spark warmup failed"
      (spark/warmup-spark :spark)))))