(ns connector.specs-test
  (:require [clojure.test :refer [deftest is testing]]
            [connector.specs :as specs]))

(def valid-local
  {:type "local"
   :format "csv"
   :link-type "public"
   :source {:path "/tmp/a.csv"}
   :options {:header true :delimiter ","}})

(deftest validate-body-positive
  (is (= valid-local (specs/validate-body! valid-local))))

(deftest validate-body-negative
  (testing "missing required source.path for local"
    (is (thrown? AssertionError
                 (specs/validate-body!
                  {:type "local"
                   :format "csv"
                   :source {}
                   :options {:header true}}))))
  (testing "private s3 requires credentials"
    (is (thrown? AssertionError
                 (specs/validate-body!
                  {:type "s3"
                   :format "csv"
                   :link-type "private"
                   :source {:bucket "b" :key "k"}
                   :auth {:access-key "only-one"}
                   :options {:header true}})))))

(deftest validate-body-edge
  (testing "private dropbox accepts revision-id"
    (is (= "dropbox"
           (:type
            (specs/validate-body!
             {:type "dropbox"
              :format "csv"
              :link-type "private"
              :source {:path "/a.csv" :revision-id "rev-2"}
              :auth {:token "tkn"}
              :options {:header true :delimiter ","}})))))
  (testing "unsupported type fails"
    (is (thrown? AssertionError
                 (specs/validate-body!
                  {:type "ftp"
                   :format "csv"
                   :source {:url "http://x"}
                   :options {:header true}})))))