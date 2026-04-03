(ns connector.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]))

(def source-types #{"local" "gdrive" "dropbox" "s3" "gcs"})
(def formats #{"csv"})
(def link-types #{"public" "private"})

(s/def ::type (s/and string? #(contains? source-types %)))
(s/def ::format (s/and string? #(contains? formats %)))
(s/def ::link_type (s/and string? #(contains? link-types %)))

(s/def ::path (s/and string? #(not (str/blank? %))))
(s/def ::url (s/and string? #(str/starts-with? % "http")))
(s/def ::bucket (s/and string? #(not (str/blank? %))))
(s/def ::key (s/and string? #(not (str/blank? %))))
(s/def ::region (s/and string? #(not (str/blank? %))))
(s/def ::revision_id string?)
(s/def ::version_id string?)
(s/def ::generation string?)

(s/def ::token (s/and string? #(not (str/blank? %))))
(s/def ::access_key (s/and string? #(not (str/blank? %))))
(s/def ::secret_key (s/and string? #(not (str/blank? %))))
(s/def ::project_id (s/and string? #(not (str/blank? %))))
(s/def ::client_email (s/and string? #(not (str/blank? %))))
(s/def ::private_key (s/and string? #(not (str/blank? %))))

(s/def ::header boolean?)
(s/def ::delimiter string?)
(s/def ::inferSchema boolean?)
(s/def ::multiLine boolean?)
(s/def ::encoding string?)
(s/def ::options (s/keys :opt-un [::header ::delimiter ::inferSchema ::multiLine ::encoding]))

(s/def ::source
  (s/keys :opt-un [::path ::url ::bucket ::key ::region
                   ::revision_id ::version_id ::generation]))

(s/def ::auth
  (s/keys :opt-un [::token ::access_key ::secret_key
                   ::project_id ::client_email ::private_key]))

(s/def ::body
  (s/keys
   :req-un [::type ::format ::source ::options]
   :opt-un [::link_type ::auth]))

(defn- private-link?
  [body]
  (= "private" (or (:link_type body) (:link-type body) "public")))

(defn- present?
  [value]
  (and (string? value) (not (str/blank? value))))

(defn- require-fields
  [m ks]
  (every? #(present? (get m %)) ks))

(defn- valid-source?
  [body]
  (let [source (:source body)
        auth (:auth body)
        private? (private-link? body)]
    (case (:type body)
      "local"
      (require-fields source [:path])

      "gdrive"
      (and (require-fields source [:url])
           (if private?
             (require-fields auth [:token])
             true))

      "dropbox"
      (if private?
        (and (require-fields source [:path])
             (require-fields auth [:token]))
        (require-fields source [:url]))

      "s3"
      (and (require-fields source [:bucket :key])
           (if private?
             (require-fields auth [:access_key :secret_key])
             true))

      "gcs"
      (and (require-fields source [:bucket :key])
           (if private?
             (require-fields auth [:project_id :client_email :private_key])
             true))

      false)))

(s/def ::valid-body
  (s/and
   ::body
   valid-source?))

(defn validate-body!
  "Validates request body and exits early on invalid input."
  [body]
  {:pre [(s/valid? ::valid-body body)]}
  body)
