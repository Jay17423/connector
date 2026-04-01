(ns connector.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]))

(def source-types #{"local" "gdrive" "dropbox" "s3" "gcs"})
(def formats #{"csv"})
(def link-types #{"public" "private"})

(s/def ::type (s/and string? #(contains? source-types %)))
(s/def ::format (s/and string? #(contains? formats %)))
(s/def ::path string?)
(s/def ::link (s/and string? #(str/starts-with? % "http")))
(s/def ::token (s/and string? #(not (str/blank? %))))
(s/def ::link-type (s/and string? #(contains? link-types %)))

(s/def ::header boolean?)
(s/def ::delimiter string?)
(s/def ::options (s/keys :opt-un [::header ::delimiter]))

(s/def ::body
  (s/keys
   :req-un [::type ::format ::link ::options]
   :opt-un [::path ::token ::revision-id ::link-type]))

(s/def ::valid-body
  (s/and
   ::body
   (fn [data]
     (let [cloud-sources #{"gdrive" "dropbox" "s3" "gcs"}]
       (if (and (contains? cloud-sources (:type data))
                (= (:link-type data) "private"))
         (contains? data :token)
         true)))))
