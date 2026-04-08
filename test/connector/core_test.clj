(ns connector.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [connector.core :as core]
            [connector.dataset :as ds]
            [connector.specs :as spec]
            [connector.utils :as util]
            [taoensso.timbre :as log]))

(deftest normalize-body
  (testing "dropbox private uses path and carries revision-id"
    (is (= {:type :dropbox
            :format "csv"
            :link-type :private
            :options {:header true}
            :cred {:link "/a.csv"
                   :token "tkn"
                   :revision-id "rev-1"}}
           (core/normalize-body
            {:type "dropbox"
             :format "csv"
             :link-type "private"
             :source {:path "/a.csv" :revision-id "rev-1"}
             :auth {:token "tkn"}
             :options {:header true}}))))
  (testing "dropbox private supports revision-id alias"
    (is (= "ver-7"
           (-> (core/normalize-body
                {:type "dropbox"
                 :format "csv"
                 :link-type "private"
                 :source {:path "/a.csv" :revision-id "ver-7"}
                 :auth {:token "tkn"}
                 :options {:header true}})
               :cred
               :revision-id))))
  (testing "gcs private-key escaped newlines are restored"
    (is (= "line1\nline2"
           (-> (core/normalize-body
                {:type "gcs"
                 :format "csv"
                 :source {:bucket "b" :key "k"}
                 :auth {:private-key "line1\\nline2"}
                 :options {}})
               :cred
               :private-key)))))

(deftest load-data-success-response
  (with-redefs [spec/validate-body! identity
                ds/read-dataset (fn [_ _] :dataset)
                util/dataset->preview (fn [_] [{:a 1}])
                log/info (fn [_])]
    (let [res (core/load-data {:body {:type "local"
                                      :format "csv"
                                      :source {:path "/tmp/a.csv"}
                                      :options {:header true}}})]
      (is (= 200 (:status res)))
      (is (= "success" (get-in res [:body :status])))
      (is (= [{:a 1}] (get-in res [:body :data]))))))

(deftest load-data-negative-and-edge
  (testing "invalid body returns 400"
    (with-redefs [spec/validate-body!
                  (fn [_] (throw (AssertionError. "bad request")))
                  log/warn (fn [_])]
      (let [res (core/load-data {:body {}})]
        (is (= 400 (:status res)))
        (is (= "error" (get-in res [:body :status]))))))
  (testing "unexpected exception returns 500"
    (with-redefs [spec/validate-body! identity
                  ds/read-dataset
                  (fn [_ _] (throw (RuntimeException. "boom")))
                  log/error (fn [_])
                  log/info (fn [_])]
      (let [res (core/load-data {:body {:type "local"
                                        :format "csv"
                                        :source {:path "/tmp/a.csv"}
                                        :options {}}})]
        (is (= 500 (:status res)))
        (is (= "Internal server error" (get-in res [:body :msg])))))))