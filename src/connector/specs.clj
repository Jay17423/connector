(ns connector.specs
  "Defines clojure.spec validations for supported data sources, authentication,
   and request structure."
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]))

(def source-types #{"local" "gdrive" "dropbox" "s3" "gcs"})
(def formats #{"csv"})
(def link-types #{"public" "private"})

(s/def ::type (s/and string? #(contains? source-types %)))
(s/def ::format (s/and string? #(contains? formats %)))
(s/def ::link-type (s/and string? #(contains? link-types %)))
(s/def ::path (s/and string? #(not (str/blank? %))))
(s/def ::url (s/and string? #(str/starts-with? % "http")))
(s/def ::bucket (s/and string? #(not (str/blank? %))))
(s/def ::key (s/and string? #(not (str/blank? %))))
(s/def ::region (s/and string? #(not (str/blank? %))))
(s/def ::revision-id string?)
(s/def ::version-id string?)
(s/def ::generation string?)
(s/def ::refresh-token (s/and string? #(not (str/blank? %))))
(s/def ::client-id (s/and string? #(not (str/blank? %))))
(s/def ::client-secret (s/and string? #(not (str/blank? %))))
(s/def ::token string?)
(s/def ::access-key (s/and string? #(not (str/blank? %))))
(s/def ::secret-key (s/and string? #(not (str/blank? %))))
(s/def ::project-id (s/and string? #(not (str/blank? %))))
(s/def ::client-email (s/and string? #(not (str/blank? %))))
(s/def ::private-key (s/and string? #(not (str/blank? %))))
(s/def ::header boolean?)
(s/def ::delimiter string?)

(s/def ::options
  (s/keys
   :opt-un
   [::header ::delimiter]))

(s/def ::source
  (s/keys
   :opt-un [::path ::url ::bucket ::key ::region ::revision-id ::version-id
            ::generation]))

(s/def ::auth
  (s/keys
   :opt-un [::token ::refresh-token ::client-id ::client-secret ::access-key
            ::secret-key ::project-id ::client-email ::private-key]))

(s/def ::body
  (s/keys
   :req-un [::type ::format ::source ::options]
   :opt-un [::link-type ::auth]))

(defn- private-link?
  "Checks if link-type is private requiring authentication."
  [body]
  (= "private" (or (:link-type body) "public")))

(defn- present?
  "Validates that string value is non-blank."
  [value]
  (and (string? value) (not (str/blank? value))))

(defn- require-fields
  "Ensures required keys exist and contain valid string values."
  [m ks]
  (every? #(present? (get m %)) ks))

(defn- valid-source?
  "Validates source configuration and required credentials based on type."
  [body]
  (let [source   (:source body)
        auth     (:auth body)
        private? (private-link? body)]

    (case (:type body)

      "local"
      (require-fields source [:path])

      "gdrive"
      (and
       (require-fields source [:url])

       (if private?
         (require-fields auth
                         [:refresh-token
                          :client-id
                          :client-secret])
         true))

      "dropbox"
      (if private?
        (and (require-fields source [:path]) (require-fields auth [:token]))
        (require-fields source [:url]))

      "s3"
      (and (require-fields source [:bucket :key])
           (if private?
             (require-fields auth [:access-key :secret-key])
             true))

      "gcs"
      (and (require-fields source [:bucket :key])
           (if private?
             (require-fields auth [:project-id :client-email :private-key])
             true))
      
      false)))

(s/def ::valid-body
  (s/and
   ::body
   valid-source?))

(defn validate-body!
  "Validates request body against spec and throws error if invalid."
  [body]
  {:pre [(s/valid? ::valid-body body)]}
  body)