(ns connector.cloud-fetch-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [connector.cloud-fetch :as fetch]
   [taoensso.timbre :as log]))

(deftest resolve-request-gdrive-public
  (testing "gdrive public link generates correct request map"
    (with-redefs [fetch/gdrive-id (fn [_] "file123")]
      (let [req (#'fetch/resolve-request*
                 :gdrive
                 {:link "https://drive.google.com/file/d/file123/view"})]
        (is (= :get (:method req)))
        (is (re-find #"drive.google.com" (:url req)))))))

(deftest resolve-request-dropbox-public
  (testing "dropbox public link converts dl=0 to dl=1"
    (let [req (#'fetch/resolve-request*
               :dropbox
               {:link "https://dropbox.com/a.csv?dl=0"})]

      (is (= :get (:method req)))
      (is (re-find #"dl=1" (:url req))))))

(deftest resolve-request-dropbox-private
  (testing "dropbox private token generates POST request"
    (let [req (#'fetch/resolve-request*
               :dropbox
               {:link "/a.csv"
                :token "abc-token"})]

      (is (= :post (:method req)))
      (is (contains? (:headers req) "Authorization")))))

(deftest fetch-file-success
  (testing "fetch-file! returns temp file path when download succeeds"

    (with-redefs
     [fetch/resolve-request*
      (fn [_ _]
        {:url "http://test.com/file.csv"
         :method :get
         :headers {}})

      fetch/build-request
      (fn [_] :mock-request)

      fetch/tmp-destination
      (fn [] "/tmp/mock.csv")

      fetch/download!
      (fn [_ _ _ _] true)
      log/info (fn [_])]
      (is (= "/tmp/mock.csv"
             (fetch/fetch-file!
              :gdrive
              {:link "test"}))))))

(deftest fetch-file-error
  (testing "fetch-file! throws exception when download fails"

    (with-redefs
     [fetch/resolve-request*
      (fn [_ _]
        {:url "http://bad-url"
         :method :get
         :headers {}})

      fetch/build-request
      (fn [_] :mock-request)

      fetch/tmp-destination
      (fn [] "/tmp/mock.csv")

      fetch/download!
      (fn [_ _ _ _]
        (throw (ex-info "download failed"
                        {:status 500})))
      log/error (fn [_])]
      (is (thrown?
           Exception
           (fetch/fetch-file!
            :gdrive
            {:link "bad"}))))))

(deftest tmp-destination-test
  (testing "tmp-destination creates csv file path"
    (let [path (#'fetch/tmp-destination)]
      (is (re-find #"cloud-raw-" path))
      (is (re-find #"\.csv" path)))))

(deftest resolve-request-public-url
  (testing "unknown source treated as public url"
    (let [req (#'fetch/resolve-request*
               :http
               {:link "http://abc.com/a.csv"})]
      (is (= :get (:method req)))
      (is (= "http://abc.com/a.csv"
             (:url req))))))